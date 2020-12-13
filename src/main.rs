use nng::{Aio, AioResult, Context, Message, Protocol, Socket};
use std::{
    convert::TryInto,
    env, process, thread,
    time::{Duration, Instant, SystemTime},
};

fn client(url: &str) -> Result<(), nng::Error> {
    // Set up the client and connect to the specified address
    let client = Socket::new(Protocol::Req0)?;
    client.dial_async(url)?;

    let ms: u64 = 10;
    let buf = ms.to_le_bytes();

    let start = Instant::now();

    const TOTAL: i32 = 100000;
    // 000
    for _ in 0..TOTAL {
        // Send the request from the client to the server. In general, it will be
        // better to directly use a `Message` to enable zero-copy, but that doesn't
        // matter here.
        let msg: Message = Message::from(buf);
        client.send(msg.clone())?;
        client.recv()?;

        // println!(
        //     "{:?}",
        //     std::str::from_utf8(msg.clone()[..].try_into().unwrap()).unwrap()
        // );
    }

    let dur = Instant::now().duration_since(start);
    let millis: u128 = dur.subsec_millis().into();
    let duration = dur.as_millis() + millis;
    println!("Request took {} millis", duration);
    println!(
        "Request per sec: {}",
        (TOTAL as f64) / ((duration / 1000) as f64)
    );

    Ok(())
}

const PARALLEL: usize = 128;

fn server(url: &str) -> Result<(), nng::Error> {
    let s = Socket::new(Protocol::Rep0)?;

    // Create all of the worker contexts
    let workers: Vec<_> = (0..PARALLEL)
        .map(|_| {
            let ctx = Context::new(&s)?;
            let ctx_clone = ctx.clone();
            let aio = Aio::new(move |aio, res| worker_callback(aio, &ctx_clone, res))?;
            Ok((aio, ctx))
        })
        .collect::<Result<_, nng::Error>>()?;

    // Only after we have the workers do we start listening.
    s.listen(url)?;

    // Now start all of the workers listening.
    for (a, c) in &workers {
        c.recv(a)?;
    }

    thread::sleep(Duration::from_secs(60 * 60 * 24 * 365));

    Ok(())
}

/// Callback function for workers.
fn worker_callback(aio: Aio, ctx: &Context, res: AioResult) {
    let msg = "Ferris".as_bytes();

    match res {
        // We successfully sent the message, wait for a new one.
        AioResult::Send(Ok(_)) => ctx.recv(&aio).unwrap(),

        // We successfully received a message.
        AioResult::Recv(Ok(_)) => {
            // let _ms = u64::from_le_bytes(m[..].try_into().unwrap());
            // println!("{}: {:?}", "rcv", ms);

            ctx.send(&aio, msg.clone()).unwrap();
        }

        // We successfully slept.
        AioResult::Sleep(Ok(_)) => unreachable!("Worker never sleeps"),

        // Anything else is an error and we will just panic.
        AioResult::Send(Err((_, e))) | AioResult::Recv(Err(e)) | AioResult::Sleep(Err(e)) => {
            panic!("Error: {}", e)
        }
    }
}

fn main() -> std::result::Result<(), nng::Error> {
    let url = "tcp://127.0.0.1:32050";

    let args: Vec<_> = env::args().collect();

    match &args[..] {
        [_, t] if t == "server" => server(url),
        // [_, t, _, _] if t == "client" => client(url, Duration::from_millis(10)),
        [_, t] if t == "client" => client(url),
        _ => {
            println!("Usage:\nasync server <url>\n  or\nasync client <url>");
            process::exit(1);
        }
    }
}
