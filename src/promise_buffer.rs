
use core::pin::Pin;
use core::future::Future;

use std::collections::VecDeque;

pub async fn promise_buffer<'a,T,F>(mut q: VecDeque<Pin<Box<dyn Future<Output = T> + std::marker::Send + 'a>>>, sz: usize, mut on_result: F)
-> () 
    where F: FnMut(T) -> ()
{    
    let mut vec: Vec<_> = Vec::new();
    loop {
        while !(vec.len() == sz) {
            match q.pop_front() {
                Some(x) => vec.push(x),
                None => break
            }
        }
        if vec.len() == 0 {
            break;
        }
        let (result, _index, z) = futures::future::select_all(vec).await;
        on_result(result);
        vec = z;
        // info!("{:#?}{:#?}{:#?}", y, vec.len() );        
    }

}