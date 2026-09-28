# Official Ubuntu 24.04 amd64 manifest. Offline sites may explicitly select a
# different digest-pinned Ubuntu base; the runner records the actual image.
ARG BASE_IMAGE=ubuntu@sha256:496754492fb28b4d3049432f2ca787449331e23fb14f0dd3fffea86bf5a93eb4
FROM ${BASE_IMAGE}
RUN apt-get update && apt-get install -y --no-install-recommends gcc libc6-dev libstdc++6 unzip ca-certificates git
ENV PATH="/toolchain/bin:${PATH}" MOON_HOME="/toolchain"
# Neither libssl-dev nor zlib1g-dev is installed. Runtime library isolation
# is checked separately in the minimal rootfs, not inferred from this image.
