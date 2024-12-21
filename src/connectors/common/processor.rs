use async_trait::async_trait;

#[async_trait]
pub trait Processor<IN, OUT>: Send + Sync + Sized + 'static {
    async fn process(&self, records: IN) -> anyhow::Result<OUT>;

    #[allow(unused_variables)]
    async fn post_stream(&self) -> anyhow::Result<()> {
        Ok(())
    }
}
