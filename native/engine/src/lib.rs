#[rustler::nif]
fn ping() -> &'static str {
    "pong from rust engine"
}

rustler::init!("Elixir.Engine.Native");
