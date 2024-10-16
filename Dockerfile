FROM rust AS builder

ARG BUILD_PROFILE=${BUILD_PROFILE:-release}

WORKDIR /src/
RUN rustup default nightly
RUN mkdir dummy
RUN echo 'fn main() {}' > dummy/dummy.rs
COPY config config
COPY data data
COPY tt tt
COPY router/Cargo.toml router/
RUN echo "[[bin]]\nname = \"dummy\"\npath = \"/src/dummy/dummy.rs\"" >> router/Cargo.toml
WORKDIR /src/router
RUN cargo build --bin dummy --profile=$BUILD_PROFILE
RUN rm -rf dummy router

WORKDIR /src/
COPY router router
RUN cargo install --path router --profile=$BUILD_PROFILE

FROM debian:trixie-slim
RUN apt-get update && apt-get install -y tini ca-certificates

COPY --from=builder /usr/local/cargo/bin/bruss_fetch /usr/local/bin/bruss_fetch
COPY --from=builder /usr/local/cargo/bin/bruss_route /usr/local/bin/bruss_route

WORKDIR /app
ENV PATH=/usr/local/cargo/bin:$PATH
CMD ["bash"]
