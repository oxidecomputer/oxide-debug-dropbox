// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! # Omicron Debug Dropbox
//!
//! The Debug Dropbox is a well-known filesystem path into which parts of
//! [Omicron][1] (the Oxide control plane) can deposit small bits of debug data
//! that will be archived for possible future use by support.  Logs and core
//! files for Omicron components are archived via other means.  The dropbox is
//! for data similar to logs and core files but that's deposited explicitly by
//! the application.  The motivating example is that whenever Reconfigurator
//! plans a new blueprint for the system, it saves a file to the dropbox
//! summarizing all the input that went into the planning process, including the
//! inventory collection, the parent blueprint, etc.  Having this information
//! available should make it straightforward to reproduce any surprising planner
//! behavior seen in the field.  For background, see [RFD 613 Debug Dropbox][2].
//!
//! **The debug dropbox should not be used in the global zone, the switch zone,
//! or any other context where /var is backed by a ramdisk.**
//!
//! [1]: https://github.com/oxidecomputer/omicron
//! [2]: https://rfd.shared.oxide.computer/rfd/0613
//!
//! ## Example
//!
//! ```no_run
//! use anyhow::Context;
//! use omicron_debug_dropbox::DebugDropbox;
//! use slog::Logger;
//!
//! # #[tokio::main]
//! # async fn main() -> Result<(), anyhow::Error> {
//! // At program startup, initialize the debug dropbox.
//! // This creates the directory if needed.
//! let log = todo!(); // create a slog Logger
//! let debug_dropbox = DebugDropbox::for_non_global_non_switch_zone(&log)
//!     .await
//!     .context("creating debug dropbox")?;
//!
//! // Also at program startup, initialize a producer.  Each producer is its own
//! // logical stream or bucket of debug data.  Use this to keep data from
//! // different components or subsystems separate from each other.
//! //
//! // This step deletes any incomplete deposits made before (e.g., due to a
//! // program crash or system reset), so a given producer name should not be
//! // used by more than one program.  (That could cause one to delete the
//! // other's live, in-flight deposits.)
//! let dropbox_reconfigurator = debug_dropbox
//!     .initialize_producer("reconfigurator")
//!     .await
//!     .context("creating dropbox producer")?;
//!
//! // When you want to save debug data, deposit it into the dropbox.
//! //
//! // It's up to you whether failure to deposit the file should be bubble
//! // out to callers.  (If this fails, is it better to proceed with the option
//! // anyway without the debug data?)
//! let filename = "my-file";
//! let contents = "what-I-was-thinking";
//! let _deposit = dropbox_reconfigurator
//!     .deposit_file_str(filename, contents)
//!     .await
//!     .context("depositing into debug dropbox")?;
//!
//! // In many cases, the debugging data is tied to a specific operation (e.g.,
//! // blueprint planning).  If the operation itself fails, you don't care about
//! // the debug data any more.  In that case, you can _try_ to cancel the
//! // deposit using the handle you got.  This is best-effort.  The data may
//! // already have been archived.
//! let deposit = dropbox_reconfigurator
//!     .deposit_file_str(filename, contents)
//!     .await
//!     .context("depositing into debug dropbox")?;
//! # let failed: bool = false;
//! // ...
//! if failed {
//!     deposit.cancel_and_attempt_delete().await;
//! }
//! # Ok(())
//! # } // tokio::main
//! ```
//!
//! ## Producer naming
//!
//! See [`DebugDropbox::initialize_producer()`].
//!
//! ## File naming
//!
//! See [`Producer::deposit_file_str()`].
//!
//! ## Deposit and archival
//!
//! When depositing data into the dropbox, it initially lands in
//! `/var/debug_dropbox`.  Sled agent archives data from this directory into the
//! system's debug datasets periodically as well as when the zone is halted or
//! after an unexpected system reset.  When the data is archived, the copy in
//! `/var/debug_dropbox` is deleted.
//!
//! While the data is being written, it's staged into a temporary directory
//! within the dropbox that is not archived by sled agent.  Partially-written
//! data should never be archived.  If the program crashes or the system resets
//! with partially-written data in the dropbox, it will be deleted when the same
//! producer is re-initialized (usually the next time the program starts up).
//!
//! ## Guidelines
//!
//! **The debug dropbox should not be used in the global zone, the switch zone,
//! or any other context where /var is backed by a ramdisk.**
//!
//! Deposited files can contain any data in any format.  Neither this crate nor
//! the archival process in sled agent looks at the contents of the files.
//!
//! The dropbox is intended for occasional, application-specific debug data that
//! can be stored in a single file.  These files are expected to be moderately
//! sized and relatively infrequent.  For concreteness, we suggest:
//!
//! * not more than 4 files in any minute
//! * not more than 1 file per hour on average
//! * not more than 10 GiB in any file
//! * not more than 10 GiB per day on average
//!
//! These are very rough numbers. They are not enforced. Rather: if you expect
//! to emit more data than this, use another mechanism to collect it.
//!
//! Crash dumps, system-generated core files, and ordinary log files do not
//! belong in the debug dropbox. Existing mechanisms already handle these.

use camino::Utf8Path;
use camino::Utf8PathBuf;
use derive_more::AsRef;
use slog::Logger;
use slog::info;
use slog::o;
use slog::warn;
use slog_error_chain::InlineErrorChain;
use std::borrow::Cow;
use std::str::FromStr;
use thiserror::Error;

/// Path to the local debug dropbox
///
/// This is determined in RFD 613 "Debug Dropbox".  This path represents a
/// cross-consolidation interface between sled agent (which collects files from
/// this directory) and components using the dropbox (which deposit them into
/// this directory).
pub static DEBUG_DROPBOX_PATH: &str = "/var/debug_dropbox";

/// Errors associated with initializing the debug dropbox
#[derive(Debug, Error)]
pub enum DropboxInitError {
    #[error("failed to create directory {0:?}")]
    Mkdir(Utf8PathBuf, #[source] std::io::Error),
}

/// Errors associated with initializing a debug dropbox producer
#[derive(Debug, Error)]
pub enum ProducerInitError {
    #[error(transparent)]
    BadBasename(#[from] BasenameError),
    #[error("producer name \"tmp\" is not allowed")]
    TmpNotAllowed,
    #[error("failed to create directory {0:?}")]
    Mkdir(Utf8PathBuf, #[source] std::io::Error),
    #[error("failed to clean up directory {0:?}")]
    Cleanup(Utf8PathBuf, #[source] std::io::Error),
}

/// Errors associated with depositing a file into the dropbox
#[derive(Debug, Error)]
pub enum DepositError {
    #[error(transparent)]
    BadName(#[from] BasenameError),
    #[error("I/O error on file {0:?}")]
    Io(Utf8PathBuf, #[source] std::io::Error),
    #[error("error renaming {0:?} to {1:?}")]
    Rename(Utf8PathBuf, Utf8PathBuf, #[source] std::io::Error),
    #[error("error fsync'ing {0:?}")]
    Fsync(Utf8PathBuf, #[source] std::io::Error),
}

/// Top-level handle to an initialized debug dropbox
///
/// Use [`DebugDropbox::initialize_producer()`] to start depositing files.
#[derive(Debug)]
pub struct DebugDropbox {
    log: Logger,
    kind: DebugDropboxKind,
}

#[derive(Debug)]
enum DebugDropboxKind {
    /// a normal debug dropbox backed by the given filesystem path
    Normal { path: Utf8PathBuf },
    /// a "no-op" debug dropbox, where deposits are ignored altogether
    /// (not written anywhere)
    Noop,
}

impl DebugDropbox {
    /// Initializes a debug dropbox for general use
    ///
    /// This function's name reflects that the dropbox is not intended for use
    /// in the global zone or the switch zone.
    pub async fn for_non_global_non_switch_zone(
        log: &Logger,
    ) -> Result<DebugDropbox, DropboxInitError> {
        DebugDropbox::new_impl(log, Utf8PathBuf::from(DEBUG_DROPBOX_PATH)).await
    }

    /// Initializes a "no-op" debug dropbox for automated tests
    ///
    /// Deposits into this dropbox are completely ignored.  They're not written
    /// out anywhere.  This is intended for use in automated tests for code that
    /// uses the debug dropbox, where you want to ignore the dropbox altogether
    /// in your tests.
    pub fn for_tests_noop(log: &Logger) -> DebugDropbox {
        let log = log.new(o!("component" => "DebugDropbox", "kind" => "noop"));
        DebugDropbox { log, kind: DebugDropboxKind::Noop }
    }

    /// Initializes a debug dropbox for automated tests where deposits are
    /// written to the specified `path` rather than `DEBUG_DROPBOX_PATH`
    ///
    /// These deposits will not be archived anywhere.
    ///
    /// This is intended for use in automated tests where you _do_ want a
    /// program's debug dropbox deposits to be saved, but either don't have a
    /// real dropbox available or don't want the files archived like they would
    /// be in the real dropbox.
    pub async fn for_tests(
        log: &Logger,
        path: &Utf8Path,
    ) -> Result<DebugDropbox, DropboxInitError> {
        DebugDropbox::new_impl(log, path.to_owned()).await
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

    /// Initialize a subdirectory of the dropbox for storing data associated
    /// with the given `producer` name
    ///
    /// Producer names should be distinct for different programs.  There must
    /// not be two programs on the system with the same producer name running at
    /// the same time.  When you initialize a producer, partially-written files
    /// associated with this producer will be deleted on the assumption that
    /// they're leftover from a crash.
    ///
    /// It's okay to use multiple different producer names within a program
    /// (e.g., for different subsystems whose data you want to keep separate).
    /// However, a given subsystem should consistently use the same name (i.e.,
    /// don't include the pid or another changing value in the producer name).
    ///
    /// The producer name will become a directory name, so it cannot be empty,
    /// cannot contain '/', and cannot be the strings `"."` or `".."`.  The
    /// producer name "tmp" is also reserved.
    pub async fn initialize_producer(
        &self,
        producer: &str,
    ) -> Result<Producer, ProducerInitError> {
        // Do the validation even for `Noop` dropboxes.

        // "tmp" is reserved for the top-level staging directory.
        if producer == "tmp" {
            return Err(ProducerInitError::TmpNotAllowed);
        }

        let producer_name = Basename::try_from(producer)?;

        match &self.kind {
            DebugDropboxKind::Noop => {
                info!(
                    &self.log,
                    "skipping producer init (noop dropbox)";
                    "producer" => producer,
                );
                Ok(Producer {
                    log: self.log.new(o!("producer" => producer.to_owned())),
                    kind: ProducerKind::Noop,
                })
            }

            DebugDropboxKind::Normal { path } => {
                let tmp_path = path.join("tmp").join(&producer_name);
                let producer_path = path.join(&producer_name);
                let log = self.log.new(o!("producer" => producer.to_owned()));
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

/// A bucket or stream of debug data, collected into one directory
///
/// Different producers' data winds up in different subdirectories within the
/// dropbox and (after archival) within the system's debug datasets.
#[derive(Debug)]
pub struct Producer {
    log: Logger,
    kind: ProducerKind,
}

#[derive(Debug)]
enum ProducerKind {
    /// a normal dropbox producer backed by the given filesystem path
    Normal { path: Utf8PathBuf, tmp_path: Utf8PathBuf },
    /// a "no-op" dropbox producer, where deposits are ignored altogether
    /// (not written anywhere)
    Noop,
}

impl Producer {
    /// Deposit a file called `name` with contents `contents` into the debug
    /// dropbox, associated with this producer
    ///
    /// If a name is duplicated within a short period (i.e., before the first
    /// one is archived), the new one may overwrite the previous one.  Archived
    /// files are given unique names, so you don't have to worry about ensuring
    /// uniqueness over all time.
    pub async fn deposit_file_str(
        &self,
        name: &str,
        contents: &str,
    ) -> Result<DepositHandle, DepositError> {
        // Validate the name even in Noop mode so that at least this part gets
        // covered in testing.
        let validated = Basename::try_from(name)?;

        match &self.kind {
            ProducerKind::Noop => {
                info!(
                    &self.log,
                    "skipping debug dropbox drop (noop impl)";
                    "name" => name,
                );
                Ok(DepositHandle::noop())
            }
            ProducerKind::Normal { path, tmp_path } => {
                let tmp_file_path = tmp_path.join(&validated);
                let final_path = path.join(&validated);

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

                Ok(DepositHandle::new(final_path, self.log.clone()))
            }
        }
    }
}

/// A filesystem path component that's not '.' or '..'
///
/// A `Basename` must not be empty, must not contain "/", and must not be either
/// of the strings "." or "..".
#[derive(AsRef, Clone, Debug)]
pub struct Basename<'a>(Cow<'a, str>);

#[derive(Debug, Clone, Error)]
#[error(
    "unsupported name \
     (empty, contains \"/\", or is \".\" or \"..\"): {0:?}"
)]
pub struct BasenameError(String);

impl<'a> AsRef<Utf8Path> for Basename<'a> {
    fn as_ref(&self) -> &Utf8Path {
        Utf8Path::new(self.0.as_ref())
    }
}

fn is_valid_basename(s: &str) -> bool {
    !s.is_empty() && !s.contains('/') && s != "." && s != ".."
}

impl<'a> FromStr for Basename<'a> {
    type Err = BasenameError;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if is_valid_basename(s) {
            Ok(Basename(Cow::Owned(s.to_owned())))
        } else {
            Err(BasenameError(s.to_owned()))
        }
    }
}

impl<'a> TryFrom<&'a str> for Basename<'a> {
    type Error = BasenameError;
    fn try_from(s: &'a str) -> Result<Self, Self::Error> {
        if is_valid_basename(s) {
            Ok(Basename(Cow::Borrowed(s)))
        } else {
            Err(BasenameError(s.to_owned()))
        }
    }
}

impl TryFrom<String> for Basename<'static> {
    type Error = BasenameError;
    fn try_from(s: String) -> Result<Self, Self::Error> {
        if is_valid_basename(&s) {
            Ok(Basename(Cow::Owned(s)))
        } else {
            Err(BasenameError(s))
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
#[derive(Debug)]
pub struct DepositHandle {
    kind: DepositHandleKind,
}

#[derive(Debug)]
enum DepositHandleKind {
    FileSaved { path: Utf8PathBuf, log: Logger },
    Noop,
}

impl DepositHandle {
    fn new(path: Utf8PathBuf, log: Logger) -> DepositHandle {
        DepositHandle { kind: DepositHandleKind::FileSaved { log, path } }
    }

    fn noop() -> DepositHandle {
        DepositHandle { kind: DepositHandleKind::Noop }
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
        let (path, log) = match self.kind {
            DepositHandleKind::FileSaved { path, log } => (path, log),
            DepositHandleKind::Noop => return,
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

    /// Test cancelling a deposit when the file has already been collected
    /// (deleted by the archiver).
    ///
    /// `cancel_and_attempt_delete` is best-effort and must not panic or
    /// return an error when the file is already gone.
    #[tokio::test]
    async fn test_cancel_already_gone() {
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

        // Simulate the archiver collecting and deleting the file.
        let deposited = test_dir.path().join("canceller").join("state.json");
        tokio::fs::remove_file(&deposited)
            .await
            .expect("failed to simulate archival");

        // cancel_and_attempt_delete should not panic even though the file
        // is already gone.
        handle.cancel_and_attempt_delete().await;

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

        for name in [".", "..", "foo/bar", ""] {
            let err = dropbox.initialize_producer(name).await.expect_err(
                &format!("producer name {name:?} should be rejected"),
            );
            assert!(
                matches!(err, ProducerInitError::BadBasename(..)),
                "expected BadBasename for producer {name:?}, got: {err:?}"
            );
            assert_eq!(
                err.to_string(),
                format!(
                    "unsupported name (empty, contains \"/\", or is \".\" \
                     or \"..\"): {name:?}",
                ),
            );
        }

        let name = "tmp";
        let err = dropbox
            .initialize_producer(name)
            .await
            .expect_err(&format!("producer name {name:?} should be rejected"));
        assert!(
            matches!(err, ProducerInitError::TmpNotAllowed),
            "expected TmpNotAllowed for producer {name:?}, got: {err:?}"
        );
        assert_eq!(
            err.to_string(),
            format!("producer name {name:?} is not allowed"),
        );
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

        for name in [".", "..", "foo/bar", ""] {
            let err = producer
                .deposit_file_str(name, "contents")
                .await
                .expect_err(&format!("filename {name:?} should be rejected"));
            assert!(
                matches!(err, DepositError::BadName(_)),
                "expected BadName for filename {name:?}, got: {err:?}"
            );
            assert_eq!(
                err.to_string(),
                format!(
                    "unsupported name (empty, contains \"/\", or is \".\" \
                     or \"..\"): {name:?}"
                ),
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
