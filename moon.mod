// Learn more about moon.mod configuration:
// https://docs.moonbitlang.com/en/latest/toolchain/moon/module.html
//
// To add a dependency, run this command in your terminal:
//   moon add moonbitlang/x
//
// Or manually declare it in `import`, for example:
// import {
//   "moonbitlang/x@0.4.6",
// }

name = "pangbit/moonpulsar"

version = "0.1.0"

readme = "README.mbt.md"

repository = ""

license = "Apache-2.0"

keywords = [ "pulsar", "messaging", "producer", "consumer" ]

preferred_target = "native"

description = "Apache Pulsar binary protocol client for MoonBit: connection management, producer (sync/async send, batching) and consumer (subscription modes, ack/nack, flow control)"

import {
  "moonbitlang/async@0.22.1",
  "moonbitlang/protobuf@0.1.3",
}
