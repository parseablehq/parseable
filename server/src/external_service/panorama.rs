use ulid::Ulid;

mod config {
    #[derive(serde::Serialize, serde::Deserialize)]
    struct Config {
        id: Ulid,
        stream: String,
        model: Model,
    }

    #[derive(serde::Serialize, serde::Deserialize)]

    enum Model {
        ZScore(Zscore),
        Sarima(Sarima),
    }

    #[derive(serde::Serialize, serde::Deserialize)]

    struct Zscore {}

    #[derive(serde::Serialize, serde::Deserialize)]

    struct Sarima {}
}
