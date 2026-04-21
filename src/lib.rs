// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

// XXX-dap TODO-doc

use camino::Utf8Path;
use camino::Utf8PathBuf;
use slog::Logger;
use slog::info;
use slog::o;
use slog::warn;
use slog_error_chain::InlineErrorChain;
use thiserror::Error;

// Determined in RFD 613 "Debug Dropbox"
pub static DEBUG_DROPBOX_PATH: &str = "/var/debug_dropbox";

#[derive(Debug, Error)]
pub enum DropboxInitError {
    #[error("failed to create directory {0:?}")]
    Mkdir(Utf8PathBuf, #[source] std::io::Error),
}

#[derive(Debug, Error)]
pub enum ProducerInitError {
    #[error(
        "producer name is not allowed (cannot be '.', '..', \
         'tmp', or contain '/'): {0:?}"
    )]
    BadName(String),
    #[error("failed to create directory {0:?}")]
    Mkdir(Utf8PathBuf, #[source] std::io::Error),
    #[error("failed to clean up directory {0:?}")]
    Cleanup(Utf8PathBuf, #[source] std::io::Error),
}

#[derive(Debug, Error)]
pub enum DepositError {
    #[error("unsupported filename (not a plain file): {0:?}")]
    BadName(String),
    #[error("I/O error on file {0:?}")]
    Io(Utf8PathBuf, #[source] std::io::Error),
    #[error("error renaming {0:?} to {1:?}")]
    Rename(Utf8PathBuf, Utf8PathBuf, #[source] std::io::Error),
    #[error("error fsync'ing {0:?}")]
    Fsync(Utf8PathBuf, #[source] std::io::Error),
}

#[derive(Debug)]
pub struct DebugDropbox {
    log: Logger,
    kind: DebugDropboxKind,
}

#[derive(Debug)]
enum DebugDropboxKind {
    Normal { path: Utf8PathBuf },
    Noop,
}

impl DebugDropbox {
    pub fn for_tests_noop(log: &Logger) -> DebugDropbox {
        let log = log.new(o!("component" => "DebugDropbox", "kind" => "noop"));
        DebugDropbox { log, kind: DebugDropboxKind::Noop }
    }

    pub async fn for_tests(
        log: &Logger,
        path: &Utf8Path,
    ) -> Result<DebugDropbox, DropboxInitError> {
        DebugDropbox::new_impl(log, path.to_owned()).await
    }

    pub async fn for_non_global_non_switch_zone(
        log: &Logger,
    ) -> Result<DebugDropbox, DropboxInitError> {
        DebugDropbox::new_impl(log, Utf8PathBuf::from(DEBUG_DROPBOX_PATH)).await
    }

    async fn new_impl(
        log: &Logger,
        path: Utf8PathBuf,
    ) -> Result<DebugDropbox, DropboxInitError> {
        let log = log
            .new(o!("component" => "DebugDropbox", "path" => path.to_string()));
        info!(log, "initializing debug dropbox");
        tokio::fs::DirBuilder::new()
            .recursive(true)
            .create(&path)
            .await
            .map_err(|error| DropboxInitError::Mkdir(path.clone(), error))?;
        Ok(DebugDropbox { log, kind: DebugDropboxKind::Normal { path } })
    }

    pub async fn initialize_producer(
        &self,
        producer: &'static str,
    ) -> Result<Producer, ProducerInitError> {
        // "tmp" is reserved for the top-level staging directory.
        if producer == "tmp" {
            return Err(ProducerInitError::BadName(producer.to_string()));
        }
        let Ok(producer_name) = Basename::new(producer) else {
            return Err(ProducerInitError::BadName(producer.to_string()));
        };

        match &self.kind {
            DebugDropboxKind::Noop => {
                info!(
                    &self.log,
                    "skipping producer init (noop dropbox)";
                    "producer" => producer,
                );
                Ok(Producer {
                    log: self.log.new(o!("producer" => producer)),
                    kind: ProducerKind::Noop,
                })
            }
            DebugDropboxKind::Normal { path } => {
                let tmp_path = path.join("tmp").join(producer_name.0);
                let producer_path = path.join(producer_name.0);
                let log = self.log.new(o!("producer" => producer));
                info!(
                    log,
                    "initializing debug dropbox producer";
                    "producer_path" => %producer_path,
                    "tmp_path" => %tmp_path,
                );

                // Delete any files left by a previous crash.
                //
                // The assumption is that the caller is the only thing that can
                // use this directory.  That means we know there are no
                // in-progress deposits.  That in turn means that anything here
                // was left over from a crash while it was being written.  Clean
                // it up.
                //
                // It's okay if the directory doesn't exist.
                match tokio::fs::remove_dir_all(&tmp_path).await {
                    Ok(()) => (),
                    Err(error)
                        if error.kind() == std::io::ErrorKind::NotFound => {}
                    Err(error) => {
                        return Err(ProducerInitError::Cleanup(
                            tmp_path, error,
                        ));
                    }
                }

                for dir in [&producer_path, &tmp_path] {
                    tokio::fs::DirBuilder::new()
                        .recursive(true)
                        .create(dir)
                        .await
                        .map_err(|error| {
                            ProducerInitError::Mkdir(dir.clone(), error)
                        })?;
                }

                Ok(Producer {
                    log,
                    kind: ProducerKind::Normal {
                        path: producer_path,
                        tmp_path,
                    },
                })
            }
        }
    }
}

#[derive(Debug)]
pub struct Producer {
    log: Logger,
    kind: ProducerKind,
}

#[derive(Debug)]
enum ProducerKind {
    Normal { path: Utf8PathBuf, tmp_path: Utf8PathBuf },
    Noop,
}

impl Producer {
    pub async fn deposit_file_str<'a>(
        &'a self,
        name: &str,
        contents: &str,
    ) -> Result<DepositHandle<'a>, DepositError> {
        // Validate the name even in Noop mode so that at least this part gets
        // covered in testing.
        let validated = Basename::new(name)?;

        match &self.kind {
            ProducerKind::Noop => {
                info!(
                    &self.log,
                    "skipping debug dropbox drop (noop impl)";
                    "name" => name,
                );
                Ok(DepositHandle::Noop)
            }
            ProducerKind::Normal { path, tmp_path } => {
                let tmp_file_path = tmp_path.join(validated.0);
                let final_path = path.join(validated.0);

                info!(
                    &self.log,
                    "saving file to debug dropbox";
                    "tmp_path" => %tmp_file_path,
                    "final_path" => %final_path,
                );

                match tokio::fs::write(&tmp_file_path, contents).await {
                    Ok(()) => (),
                    Err(error) => {
                        return Err(DepositError::Io(tmp_file_path, error));
                    }
                };

                tokio::fs::rename(&tmp_file_path, &final_path).await.map_err(
                    |error| {
                        DepositError::Rename(
                            tmp_file_path,
                            final_path.clone(),
                            error,
                        )
                    },
                )?;

                // fsync everything in sight.
                for fsync_path in [&final_path, path] {
                    tokio::fs::File::open(fsync_path)
                        .await
                        .map_err(|error| {
                            DepositError::Fsync(fsync_path.to_owned(), error)
                        })?
                        .sync_all()
                        .await
                        .map_err(|error| {
                            DepositError::Fsync(fsync_path.to_owned(), error)
                        })?;
                }

                Ok(DepositHandle::new(final_path, &self.log))
            }
        }
    }
}

#[derive(Debug)]
struct Basename<'a>(&'a str);
impl<'a> Basename<'a> {
    fn new(s: &'a str) -> Result<Basename<'a>, DepositError> {
        if s.contains("/") || s == "." || s == ".." {
            Err(DepositError::BadName(s.to_owned()))
        } else {
            Ok(Basename(s))
        }
    }
}

/// Handle that allows callers to try to remove something they've deposited into
/// the dropbox.
///
/// This is useful for cases where you want to put something in the dropbox
/// before attempting some other operation (so that you're guaranteed to have
/// the debug data for it), but then that operation fails.  You can attempt to
/// remove the file in this case to avoid saving useless data, but the
/// assumption is that it's not a problem if the data was already collected.
pub enum DepositHandle<'a> {
    FileSaved { path: Utf8PathBuf, log: &'a Logger },
    Noop,
}

impl<'a> DepositHandle<'a> {
    fn new(path: Utf8PathBuf, log: &'a Logger) -> DepositHandle<'a> {
        DepositHandle::FileSaved { log, path }
    }

    /// Makes a best-effort to delete the file from the dropbox before it gets
    /// picked up.
    ///
    /// The file may have already been picked up and there is no way to tell.
    ///
    /// This function exists for cases where the caller has determined that the
    /// debug data is no longer useful, but the assumption is that it's not a
    /// problem if it got saved anyway.
    pub async fn cancel_and_attempt_delete(self) {
        let (path, log) = match self {
            DepositHandle::FileSaved { path, log } => (path, log),
            DepositHandle::Noop => return,
        };

        let path_str = path.to_string();
        match tokio::fs::remove_file(&path).await {
            Ok(()) => {
                info!(log, "cancelled debug dropbox file"; "path" => &path_str);
            }
            Err(error) => {
                let error = InlineErrorChain::new(&error);
                warn!(
                    log,
                    "failed to cancel debug dropbox file";
                    "path" => &path_str,
                    error
                );
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use camino::Utf8Path;
    use camino_tempfile::Utf8TempDir;
    use slog::{Logger, o};

    use super::{DebugDropbox, DepositError, ProducerInitError};

    fn new_test_log() -> Logger {
        use slog::Drain;
        let decorator =
            slog_term::PlainSyncDecorator::new(slog_term::TestStdoutWriter);
        let drain = slog_term::FullFormat::new(decorator).build().fuse();
        Logger::root(drain, o!())
    }

    /// Temporary directory for tests, preserved on failure.
    ///
    /// The path is printed to stderr on creation.  Call `cleanup()` at
    /// the end of a successful test to delete it.  If `cleanup()` is never
    /// called (e.g., the test panicked), the directory is preserved for
    /// inspection.
    struct TestDir {
        dir: Option<Utf8TempDir>,
    }

    impl TestDir {
        fn new() -> Self {
            let dir =
                camino_tempfile::tempdir().expect("failed to create temp dir");
            eprintln!("test directory: {}", dir.path());
            TestDir { dir: Some(dir) }
        }

        fn path(&self) -> &Utf8Path {
            self.dir.as_ref().unwrap().path()
        }

        /// Signal success and clean up the temp directory.
        ///
        /// Only call this once the test has fully succeeded.
        fn cleanup(mut self) {
            drop(self.dir.take());
        }
    }

    impl Drop for TestDir {
        fn drop(&mut self) {
            if let Some(dir) = self.dir.take() {
                let path = dir.keep();
                eprintln!(
                    "test directory preserved (test may have \
                     failed): {path}"
                );
            }
        }
    }

    /// Test creating a dropbox and depositing a couple of files.
    ///
    /// Verifies that files land in `{dropbox}/{producer}/{filename}`.
    #[tokio::test]
    async fn test_deposit_basic() {
        let log = new_test_log();
        let test_dir = TestDir::new();

        let dropbox = DebugDropbox::for_tests(&log, test_dir.path())
            .await
            .expect("failed to create dropbox");
        let producer = dropbox
            .initialize_producer("nexus")
            .await
            .expect("failed to init producer");

        let blueprint_contents = r#"{"key":"val"}"#;
        let state_contents = "some state data";

        let _h1 = producer
            .deposit_file_str("blueprint.json", blueprint_contents)
            .await
            .expect("failed to deposit blueprint.json");
        let _h2 = producer
            .deposit_file_str("state.txt", state_contents)
            .await
            .expect("failed to deposit state.txt");

        let file1 = test_dir.path().join("nexus").join("blueprint.json");
        assert!(file1.exists(), "expected file at {file1}");
        assert_eq!(
            std::fs::read_to_string(&file1).unwrap(),
            blueprint_contents,
        );

        let file2 = test_dir.path().join("nexus").join("state.txt");
        assert!(file2.exists(), "expected file at {file2}");
        assert_eq!(std::fs::read_to_string(&file2).unwrap(), state_contents,);

        test_dir.cleanup();
    }

    /// Test that two producers can share the same dropbox directory.
    ///
    /// Both deposits should succeed and produce distinct files.
    #[tokio::test]
    async fn test_two_producers_same_directory() {
        let log = new_test_log();
        let test_dir = TestDir::new();

        let dropbox1 = DebugDropbox::for_tests(&log, test_dir.path())
            .await
            .expect("failed to create dropbox");
        let p1 = dropbox1
            .initialize_producer("p1")
            .await
            .expect("failed to init p1");
        let dropbox2 = DebugDropbox::for_tests(&log, test_dir.path())
            .await
            .expect("failed to create dropbox");
        let p2 = dropbox2
            .initialize_producer("p2")
            .await
            .expect("failed to init p2");

        let p1_contents = "p1 data";
        let p2_contents = "p2 data";

        let _h1 = p1
            .deposit_file_str("data.txt", p1_contents)
            .await
            .expect("p1 failed to deposit");
        let _h2 = p2
            .deposit_file_str("data.txt", p2_contents)
            .await
            .expect("p2 failed to deposit");

        let file1 = test_dir.path().join("p1").join("data.txt");
        assert!(file1.exists(), "expected p1 file at {file1}");
        assert_eq!(std::fs::read_to_string(&file1).unwrap(), p1_contents,);

        let file2 = test_dir.path().join("p2").join("data.txt");
        assert!(file2.exists(), "expected p2 file at {file2}");
        assert_eq!(std::fs::read_to_string(&file2).unwrap(), p2_contents,);

        test_dir.cleanup();
    }

    /// Test that initializing a producer cleans up only that producer's
    /// temporary files, leaving another producer's tmp files untouched.
    ///
    /// This simulates the crash-recovery case: p1 has stale files left over
    /// from a previous crashed write, while p2 has an in-progress write.
    /// Initializing p1's producer should remove p1's stale data without
    /// touching p2's.
    #[tokio::test]
    async fn test_cleanup_on_init() {
        let log = new_test_log();
        let test_dir = TestDir::new();

        // Simulate a previous crashed write for p1.
        let p1_tmp = test_dir.path().join("tmp").join("p1");
        tokio::fs::DirBuilder::new()
            .recursive(true)
            .create(&p1_tmp)
            .await
            .expect("failed to create p1 tmp dir");
        let p1_stale = p1_tmp.join("stale.json");
        tokio::fs::write(&p1_stale, "stale data")
            .await
            .expect("failed to write p1 stale file");

        // Simulate an in-progress write for p2.
        let p2_tmp = test_dir.path().join("tmp").join("p2");
        tokio::fs::DirBuilder::new()
            .recursive(true)
            .create(&p2_tmp)
            .await
            .expect("failed to create p2 tmp dir");
        let p2_in_progress = p2_tmp.join("state.json");
        tokio::fs::write(&p2_in_progress, "p2 in-progress data")
            .await
            .expect("failed to write p2 in-progress file");

        // Initializing p1's producer should clean up p1's tmp dir.
        let dropbox = DebugDropbox::for_tests(&log, test_dir.path())
            .await
            .expect("failed to create dropbox");
        let _p1 = dropbox
            .initialize_producer("p1")
            .await
            .expect("failed to init p1 producer");

        assert!(
            !p1_stale.exists(),
            "p1 stale file should have been removed on init"
        );
        assert!(
            p2_in_progress.exists(),
            "p2 in-progress file should be untouched"
        );

        test_dir.cleanup();
    }

    /// Test cancelling a deposited file before it is collected.
    ///
    /// In a test environment there is no archiver running, so the cancel
    /// is guaranteed to find the file still present and remove it.
    #[tokio::test]
    async fn test_cancel() {
        let log = new_test_log();
        let test_dir = TestDir::new();

        let dropbox = DebugDropbox::for_tests(&log, test_dir.path())
            .await
            .expect("failed to create dropbox");
        let producer = dropbox
            .initialize_producer("canceller")
            .await
            .expect("failed to init producer");

        let handle = producer
            .deposit_file_str("state.json", "{}")
            .await
            .expect("failed to deposit");

        let deposited = test_dir.path().join("canceller").join("state.json");
        assert!(
            deposited.exists(),
            "file should exist before cancel: {deposited}"
        );

        handle.cancel_and_attempt_delete().await;

        assert!(
            !deposited.exists(),
            "file should be gone after cancel: {deposited}"
        );

        test_dir.cleanup();
    }

    /// Test that invalid producer names are rejected.
    ///
    /// The noop dropbox validates names before checking the dropbox kind,
    /// so no filesystem setup is needed.
    #[tokio::test]
    async fn test_invalid_producer_name() {
        let log = new_test_log();
        let dropbox = DebugDropbox::for_tests_noop(&log);

        for name in [".", "..", "foo/bar", "tmp"] {
            let err = dropbox.initialize_producer(name).await.expect_err(
                &format!("producer name {name:?} should be rejected"),
            );
            assert!(
                matches!(err, ProducerInitError::BadName(_)),
                "expected BadName for producer {name:?}, got: {err:?}"
            );
            assert_eq!(
                err.to_string(),
                format!(
                    "producer name is not allowed (cannot be '.', \
                     '..', 'tmp', or contain '/'): {name:?}"
                ),
            );
        }
    }

    /// Test that invalid filenames are rejected when depositing.
    ///
    /// The noop producer validates names, so no filesystem setup
    /// is needed.
    #[tokio::test]
    async fn test_invalid_filename() {
        let log = new_test_log();
        let dropbox = DebugDropbox::for_tests_noop(&log);
        let producer = dropbox
            .initialize_producer("test-producer")
            .await
            .expect("noop producer init should succeed");

        for name in [".", "..", "foo/bar"] {
            // Use .err().unwrap_or_else() rather than .expect_err()
            // because the latter requires DepositHandle: Debug.
            let err = producer
                .deposit_file_str(name, "contents")
                .await
                .err()
                .unwrap_or_else(|| {
                    panic!("filename {name:?} should be rejected")
                });
            assert!(
                matches!(err, DepositError::BadName(_)),
                "expected BadName for filename {name:?}, got: {err:?}"
            );
            assert_eq!(
                err.to_string(),
                format!("unsupported filename (not a plain file): {name:?}"),
            );
        }
    }

    /// Smoke test for the noop dropbox: deposit succeeds and cancel is
    /// a no-op.
    #[tokio::test]
    async fn test_noop() {
        let log = new_test_log();
        let dropbox = DebugDropbox::for_tests_noop(&log);
        let producer = dropbox
            .initialize_producer("noop-producer")
            .await
            .expect("noop producer init should succeed");

        let handle = producer
            .deposit_file_str("debug.json", "{}")
            .await
            .expect("noop deposit should succeed");

        // cancel_and_attempt_delete on a Noop handle should be silent.
        handle.cancel_and_attempt_delete().await;
    }
}
