// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Deposits a file into the debug dropbox.

use anyhow::Context;
use anyhow::Result;
use anyhow::anyhow;
use camino::Utf8PathBuf;
use clap::Parser;
use omicron_debug_dropbox::DebugDropbox;
use slog::Drain;
use slog::Logger;
use slog::o;

#[derive(Debug, Parser)]
#[command(about = "Deposit a file into the debug dropbox")]
struct Args {
    /// Use this directory as the dropbox root instead of the system default.
    ///
    /// The directory is created if it does not already exist.
    #[arg(long)]
    dropbox_path: Option<Utf8PathBuf>,

    /// Name to use for the deposited file (default: basename of FILENAME)
    #[arg(long)]
    file_name: Option<String>,

    /// Producer name, identifying the subsystem depositing the data
    #[arg(long)]
    producer: String,

    /// Path to the file to deposit
    filename: Utf8PathBuf,
}

fn build_log() -> Logger {
    let decorator = slog_term::TermDecorator::new().build();
    let drain = slog_term::FullFormat::new(decorator).build().fuse();
    let drain = std::sync::Mutex::new(drain).fuse();
    Logger::root(drain, o!())
}

#[tokio::main]
async fn main() {
    let args = Args::parse();
    let log = build_log();
    if let Err(e) = run(args, log).await {
        eprintln!("error: {:#}", e);
        std::process::exit(1);
    }
}

async fn run(args: Args, log: Logger) -> Result<()> {
    // Determine the name for the deposited file.
    let deposit_name = match args.file_name {
        Some(ref name) => name.clone(),
        None => args
            .filename
            .file_name()
            .ok_or_else(|| {
                anyhow!(
                    "could not determine a filename from {:?}; \
                     use --file-name to specify one explicitly",
                    args.filename
                )
            })?
            .to_owned(),
    };

    // Read the file to deposit.
    let contents = tokio::fs::read_to_string(&args.filename)
        .await
        .with_context(|| format!("read {:?}", args.filename))?;

    // Initialize the dropbox.
    let dropbox = match args.dropbox_path {
        Some(ref path) => DebugDropbox::for_tests(&log, path)
            .await
            .context("initialize dropbox")?,
        None => DebugDropbox::for_non_global_non_switch_zone(&log)
            .await
            .context("initialize dropbox")?,
    };

    // Initialize the producer.
    let producer = dropbox
        .initialize_producer(&args.producer)
        .await
        .context("initialize producer")?;

    // Deposit the file.
    producer.deposit_file_str(&deposit_name, &contents).await.with_context(
        || format!("deposit {:?} as {:?}", args.filename, deposit_name),
    )?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::Args;
    use super::run;
    use camino::Utf8Path;
    use camino_tempfile::Utf8TempDir;
    use clap::Parser;
    use slog::Logger;
    use slog::o;

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

    /// Test basic deposit: the file lands at
    /// `{dropbox}/{producer}/{basename}` with the expected contents.
    #[tokio::test]
    async fn test_deposit_basename() {
        let dropbox_dir = TestDir::new();
        let file_dir = TestDir::new();
        let src = file_dir.path().join("state.json");
        tokio::fs::write(&src, r#"{"key":"val"}"#)
            .await
            .expect("failed to write source file");

        let args = Args::try_parse_from([
            "debug-dropbox-deposit",
            "--dropbox-path",
            dropbox_dir.path().as_str(),
            "--producer",
            "nexus",
            src.as_str(),
        ])
        .expect("failed to parse args");
        run(args, new_test_log()).await.expect("run failed");

        let deposited = dropbox_dir.path().join("nexus").join("state.json");
        assert!(deposited.exists(), "expected file at {deposited}");
        assert_eq!(
            std::fs::read_to_string(&deposited).unwrap(),
            r#"{"key":"val"}"#,
        );

        file_dir.cleanup();
        dropbox_dir.cleanup();
    }

    /// Test that --file-name overrides the basename of the source file.
    #[tokio::test]
    async fn test_deposit_custom_filename() {
        let dropbox_dir = TestDir::new();
        let file_dir = TestDir::new();
        let src = file_dir.path().join("state.json");
        tokio::fs::write(&src, "contents")
            .await
            .expect("failed to write source file");

        let args = Args::try_parse_from([
            "debug-dropbox-deposit",
            "--dropbox-path",
            dropbox_dir.path().as_str(),
            "--producer",
            "nexus",
            "--file-name",
            "custom-name.json",
            src.as_str(),
        ])
        .expect("failed to parse args");
        run(args, new_test_log()).await.expect("run failed");

        let deposited =
            dropbox_dir.path().join("nexus").join("custom-name.json");
        assert!(deposited.exists(), "expected file at {deposited}");
        assert_eq!(std::fs::read_to_string(&deposited).unwrap(), "contents");

        file_dir.cleanup();
        dropbox_dir.cleanup();
    }

    /// Test that a missing source file produces an error that mentions
    /// the read operation.
    #[tokio::test]
    async fn test_missing_source_file() {
        let dropbox_dir = TestDir::new();

        let args = Args::try_parse_from([
            "debug-dropbox-deposit",
            "--dropbox-path",
            dropbox_dir.path().as_str(),
            "--producer",
            "nexus",
            "/nonexistent/path/state.json",
        ])
        .expect("failed to parse args");

        let err =
            run(args, new_test_log()).await.expect_err("expected an error");
        assert!(
            err.to_string().contains("read"),
            "expected 'read' in error message, got: {err:#}",
        );

        dropbox_dir.cleanup();
    }

    /// Test that an invalid producer name produces an error that mentions
    /// the producer initialization.
    #[tokio::test]
    async fn test_invalid_producer() {
        let dropbox_dir = TestDir::new();
        let file_dir = TestDir::new();
        let src = file_dir.path().join("state.json");
        tokio::fs::write(&src, "contents")
            .await
            .expect("failed to write source file");

        let args = Args::try_parse_from([
            "debug-dropbox-deposit",
            "--dropbox-path",
            dropbox_dir.path().as_str(),
            "--producer",
            "bad/name",
            src.as_str(),
        ])
        .expect("failed to parse args");

        let err =
            run(args, new_test_log()).await.expect_err("expected an error");
        assert!(
            err.to_string().contains("initialize producer"),
            "expected 'initialize producer' in error message, got: {err:#}",
        );

        file_dir.cleanup();
        dropbox_dir.cleanup();
    }

    /// Test that a path with no basename (e.g., "/") produces an error
    /// suggesting --file-name when that option was not given.
    #[tokio::test]
    async fn test_no_basename() {
        let dropbox_dir = TestDir::new();

        let args = Args::try_parse_from([
            "debug-dropbox-deposit",
            "--dropbox-path",
            dropbox_dir.path().as_str(),
            "--producer",
            "nexus",
            "/",
        ])
        .expect("failed to parse args");

        let err =
            run(args, new_test_log()).await.expect_err("expected an error");
        assert!(
            err.to_string().contains("--file-name"),
            "expected '--file-name' in error message, got: {err:#}",
        );

        dropbox_dir.cleanup();
    }
}
