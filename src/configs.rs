use bruss_config::BrussConfig;
use lazy_static::lazy_static;

lazy_static! {
    pub static ref CONFIGS: BrussConfig = BrussConfig::from_file("bruss.toml").expect("cannot load configs");
}
