
pub mod lookup_butcher;
pub mod lookup_search_tree;
pub mod merge_search_trees;
pub mod demolish_search_tree;

pub enum TaskArgs {
    LookupButcher(lookup_butcher::Args),
    LookupSearchTree(lookup_search_tree::Args),
    MergeSearchTrees(merge_search_trees::Args),
    DemolishSearchTree(demolish_search_tree::Args),
}

pub enum TaskDone {
    LookupButcher(lookup_butcher::Done),
    LookupSearchTree(lookup_search_tree::Done),
    MergeSearchTrees(merge_search_trees::Done),
    DemolishSearchTree(demolish_search_tree::Done),
}

#[derive(Debug)]
pub enum Error {
    LookupButcher(lookup_butcher::Error),
    LookupSearchTree(lookup_search_tree::Error),
    MergeSearchTrees(merge_search_trees::Error),
    DemolishSearchTree(demolish_search_tree::Error),
}

pub async fn run_args(args: TaskArgs) -> Result<TaskDone, Error> {
    Ok(match args {
        TaskArgs::LookupButcher(args) =>
            TaskDone::LookupButcher(
                lookup_butcher::run(args).await
                    .map_err(Error::LookupButcher)?,
            ),
        TaskArgs::LookupSearchTree(args) =>
            TaskDone::LookupSearchTree(
                lookup_search_tree::run(args).await
                    .map_err(Error::LookupSearchTree)?,
            ),
        TaskArgs::MergeSearchTrees(args) =>
            TaskDone::MergeSearchTrees(
                merge_search_trees::run(args).await
                    .map_err(Error::MergeSearchTrees)?,
            ),
        TaskArgs::DemolishSearchTree(args) =>
            TaskDone::DemolishSearchTree(
                demolish_search_tree::run(args).await
                    .map_err(Error::DemolishSearchTree)?,
            ),
    })
}
