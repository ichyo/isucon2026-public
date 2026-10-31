use std::path::PathBuf;

fn main() {
    let mut out = PathBuf::from("testdata/small");
    let mut scale = gen_testdata::Scale::Small;
    let mut args = std::env::args().skip(1);
    while let Some(a) = args.next() {
        match a.as_str() {
            "--out" => out = PathBuf::from(args.next().expect("--out DIR")),
            "--scale" => {
                scale = match args.next().as_deref() {
                    Some("small") => gen_testdata::Scale::Small,
                    Some("full") => gen_testdata::Scale::Full,
                    other => panic!("--scale small|full (got {other:?})"),
                }
            }
            "-h" | "--help" => {
                eprintln!("usage: gen-testdata [--out DIR] [--scale small|full]");
                return;
            }
            other => panic!("unknown argument {other}"),
        }
    }
    let t = std::time::Instant::now();
    let s = gen_testdata::generate(&out, scale).expect("generate");
    eprintln!(
        "wrote {} : users={} posts={} realmojis={} comments={} images={} in {:.1?}",
        out.display(),
        s.users,
        s.posts,
        s.realmojis,
        s.comments,
        s.images,
        t.elapsed()
    );
}
