pub mod retrieve_value;

pub enum TaskArgs {
    RetrieveValue(retrieve_value::Args),
}

pub enum TaskDone {
    RetrieveValue(retrieve_value::Done),
}

#[derive(Debug)]
pub enum Error {
    RetrieveValue(retrieve_value::Error),
}

pub async fn run_args(args: TaskArgs) -> Result<TaskDone, Error> {
    Ok(match args {
        TaskArgs::RetrieveValue(args) =>
            TaskDone::RetrieveValue(
                retrieve_value::run(args).await
                    .map_err(Error::RetrieveValue)?,
            ),
    })
}
