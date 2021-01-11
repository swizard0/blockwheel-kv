use futures::{
    select,
    stream::{
        FusedStream,
        FuturesUnordered,
    },
    channel::{
        oneshot,
    },
    StreamExt,
};

use crate::{
    kv,
    wheels,
    storage,
    blockwheel,
    core::{
        search_tree::{
            Demolished,
            KeyValueRef,
            SearchTreeIterItemsRx,
        },
    },
};

pub struct Args {
    pub done_reply_tx: oneshot::Sender<Demolished>,
    pub block_items_reply_rx: oneshot::Receiver<SearchTreeIterItemsRx>,
    pub wheels_pid: wheels::Pid,
    pub remove_tasks_limit: usize,
}

pub struct Done {
    pub blocks_deleted: usize,
    pub done_reply_tx: oneshot::Sender<Demolished>,
}

#[derive(Debug)]
pub enum Error {
    IterPeerDisconnected,
    WheelsGone,
    WheelNotFound {
        blockwheel_filename: wheels::WheelFilename,
    },
    DeleteBlock(blockwheel::DeleteBlockError),
}

pub async fn run(Args { done_reply_tx, block_items_reply_rx, wheels_pid, remove_tasks_limit, }: Args) -> Result<Done, Error> {
    log::debug!("spawned task with remove_tasks_limit = {:?}", remove_tasks_limit);

    let SearchTreeIterItemsRx { items_rx, } = block_items_reply_rx.await
        .map_err(|oneshot::Canceled| Error::IterPeerDisconnected)?;
    let mut fused_block_items_rx = block_items_reply_rx.fuse();

    let mut remove_tasks = FuturesUnordered::new();
    let mut remove_tasks_count = 0;
    let mut blocks_deleted = 0;
    let mut items_depleted = false;

    loop {
        enum Event<B, T> {
            BlockItem(Option<B>),
            RemoveTask(T),
        }
        let event = if remove_tasks_count == 0 {
            Event::BlockItem(fused_block_items_rx.next().await)
        } else if remove_tasks_count < remove_tasks_limit {
            select! {
                result = fused_block_items_rx.next() =>
                    Event::BlockItem(result),
                result = remove_tasks.next() => match result {
                    None =>
                        unreachable!(),
                    Some(remove_task) => {
                        remove_tasks_count -= 1;
                        Event::RemoveTask(remove_task)
                    },
                },
            }
        } else {
            let remove_task = remove_tasks.next().await
                .unwrap();
            remove_tasks_count -= 1;
            Event::RemoveTask(remove_task)
        };

        match event {
            Event::BlockItem(None) => {
                assert!(items_depleted);
            },

            Event::BlockItem(Some(KeyValueRef::NoMore)) =>
                items_depleted = true,

            Event::BlockItem(Some(KeyValueRef::Item { value_cell: kv::ValueCell::Value(storage::OwnedValueBlockRef::Ref(block_ref)), .. })) =>
                todo!(),

            Event::BlockItem(Some(KeyValueRef::Item { .. })) =>
                todo!(),

            Event::BlockItem(Some(KeyValueRef::BlockFinish(block_ref))) => {
                let mut wheels_pid = wheels_pid.clone();
                remove_tasks.push(async move {
                    let mut wheel_ref = wheels_pid.get(block_ref.blockwheel_filename.clone()).await
                        .map_err(|ero::NoProcError| Error::WheelsGone)?
                        .ok_or_else(|| Error::WheelNotFound {
                            blockwheel_filename: block_ref.blockwheel_filename.clone(),
                        })?;
                    let blockwheel::Deleted = wheel_ref.blockwheel_pid.delete_block(block_ref.block_id).await
                        .map_err(Error::DeleteBlock)?;
                    Ok(())
                });
                remove_tasks_count += 1;
            },

            Event::RemoveTask(status) => {
                let () = status?;
                blocks_deleted += 1;
                if remove_tasks_count == 0 && fused_block_items_rx.is_terminated() {
                    return Ok(Done { blocks_deleted, done_reply_tx, });
                }
            },
        }
    }
}
