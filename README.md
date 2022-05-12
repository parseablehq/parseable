# Parseable

![Lines of Code][s6]

- On MAC, Run minio server locally. 
```
wget https://dl.min.io/server/minio/release/darwin-amd64/minio
chmod +x minio
MINIO_ROOT_USER=admin MINIO_ROOT_PASSWORD=password ./minio server /tmp --console-address ":9001"
```

- Create a bucket ```67111b0f870e443ca59200b51221243b```

- Run parseable locally, server runs on ```:5678``` port.

```
make run
```

[s6]: https://tokei.rs/b1/github/crossterm-rs/crossterm?category=code