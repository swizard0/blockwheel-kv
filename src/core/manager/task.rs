
pub mod info_butcher;
pub mod insert_butcher;
pub mod lookup_butcher;
pub mod remove_butcher;
pub mod flush_butcher;
pub mod info_search_tree;
pub mod lookup_search_tree;
pub mod flush_search_tree;
pub mod merge_search_trees;
pub mod demolish_search_tree;

pub enum TaskArgs {
    InfoButcher(info_butcher::Args),
    InsertButcher(insert_butcher::Args),
    LookupButcher(lookup_butcher::Args),
    RemoveButcher(remove_butcher::Args),
    FlushButcher(flush_butcher::Args),
    InfoSearchTree(info_search_tree::Args),
    LookupSearchTree(lookup_search_tree::Args),
    FlushSearchTree(flush_search_tree::Args),
    MergeSearchTrees(merge_search_trees::Args),
    DemolishSearchTree(demolish_search_tree::Args),
}

pub enum TaskDone {
    InfoButcher(info_butcher::Done),
    InsertButcher(insert_butcher::Done),
    LookupButcher(lookup_butcher::Done),
    RemoveButcher(remove_butcher::Done),
    FlushButcher(flush_butcher::Done),
    InfoSearchTree(info_search_tree::Done),
    LookupSearchTree(lookup_search_tree::Done),
    FlushSearchTree(flush_search_tree::Done),
    MergeSearchTrees(merge_search_trees::Done),
    DemolishSearchTree(demolish_search_tree::Done),
}

#[derive(Debug)]
pub enum Error {
    InfoButcher(info_butcher::Error),
    InsertButcher(insert_butcher::Error),
    LookupButcher(lookup_butcher::Error),
    RemoveButcher(remove_butcher::Error),
    FlushButcher(flush_butcher::Error),
    InfoSearchTree(info_search_tree::Error),
    LookupSearchTree(lookup_search_tree::Error),
    FlushSearchTree(flush_search_tree::Error),
    MergeSearchTrees(merge_search_trees::Error),
    DemolishSearchTree(demolish_search_tree::Error),
}

pub async fn run_args(args: TaskArgs) -> Result<TaskDone, Error> {
    Ok(match args {
        TaskArgs::InfoButcher(args) =>
            TaskDone::InfoButcher(
                info_butcher::run(args).await
                    .map_err(Error::InfoButcher)?,
            ),
        TaskArgs::InsertButcher(args) =>
            TaskDone::InsertButcher(
                insert_butcher::run(args).await
                    .map_err(Error::InsertButcher)?,
            ),
        TaskArgs::LookupButcher(args) =>
            TaskDone::LookupButcher(
                lookup_butcher::run(args).await
                    .map_err(Error::LookupButcher)?,
            ),
        TaskArgs::RemoveButcher(args) =>
            TaskDone::RemoveButcher(
                remove_butcher::run(args).await
                    .map_err(Error::RemoveButcher)?,
            ),
        TaskArgs::FlushButcher(args) =>
            TaskDone::FlushButcher(
                flush_butcher::run(args).await
                    .map_err(Error::FlushButcher)?,
            ),
        TaskArgs::InfoSearchTree(args) =>
            TaskDone::InfoSearchTree(
                info_search_tree::run(args).await
                    .map_err(Error::InfoSearchTree)?,
            ),
        TaskArgs::LookupSearchTree(args) =>
            TaskDone::LookupSearchTree(
                lookup_search_tree::run(args).await
                    .map_err(Error::LookupSearchTree)?,
            ),
        TaskArgs::FlushSearchTree(args) =>
            TaskDone::FlushSearchTree(
                flush_search_tree::run(args).await
                    .map_err(Error::FlushSearchTree)?,
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
