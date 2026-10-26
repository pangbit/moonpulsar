// Live entry point is opt-in. `moon test` does not execute this main function.

///|
async fn main {
  guard @env.get_env_var("MP_PENDING_ENABLE") == Some("1") else {
    raise FixtureError::Rejected(
      "explicit MP_PENDING_ENABLE=1 is required for this live fixture",
    )
  }
  let run_id = @env.get_env_var("MP_PENDING_RUN_ID").unwrap_or("")
  guard valid_nonce(run_id) else {
    raise FixtureError::Rejected("32 lowercase hex run id required")
  }
  let backend = loopback_endpoint(
    @env.get_env_var("MP_PENDING_BACKEND").unwrap_or(""),
  )
  // No configurable input paths, credentials, TLS or nonliteral hosts.
  let directory = "/tmp/moonpulsar-pending-" + run_id
  if @env.get_env_var("MP_PENDING_PRECREATED_ROOT") == Some("1") {
    guard @fs.kind(directory, follow_symlink=false) == @fs.FileKind::Directory &&
      @fs.kind(directory + "/owner-token", follow_symlink=false) ==
      @fs.FileKind::Regular &&
      @fs.file_size(directory + "/owner-token") == 32L &&
      @fs.read_file(directory + "/owner-token").text() == run_id else {
      raise FixtureError::Rejected("precreated root ownership mismatch")
    }
  } else {
    @fs.mkdir(directory, permission=0o700)
  }
  let journal : Journal = {
    directory,
    run_id,
    mutex: @async.Mutex::Mutex(),
    next: 0,
  }
  @async.with_timeout(240000, async fn() {
    @async.with_task_group(async fn(group) {
      let runtime = start_fixture_runtime(group, backend, journal)
      defer runtime.server.close()
      ignore(runtime.run_client(group))
    })
  })
}
