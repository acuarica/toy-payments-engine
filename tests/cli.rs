use std::process::Command;

use assert_cmd::prelude::{CommandCargoExt, OutputAssertExt};
use predicates::prelude::predicate;

fn bin() -> Command {
    Command::cargo_bin("toy-payments-engine").unwrap()
}

#[test]
fn usage_with_no_args() {
    bin()
        .assert()
        .failure()
        .stderr(predicate::str::contains("Usage: "));
}

#[test]
fn usage_with_multiple_args() {
    bin()
        .arg("foobar")
        .arg("test/file/doesnt/exist")
        .assert()
        .failure()
        .stderr(predicate::str::contains("Usage: "));
}

#[test]
fn path_doesnt_exist() {
    bin()
        .arg("file/path/doesnt/exist")
        .assert()
        .failure()
        .stderr(predicate::str::contains("No such file or directory"));
}

#[test]
fn path_exist() {
    bin()
        .arg("./input-example.csv")
        .assert()
        .success()
        .stdout(predicate::str::contains("client,available,held,total,locked"));
}
