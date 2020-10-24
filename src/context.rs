use std::fmt::Debug;

pub trait Context {
    type Info;
    type Insert: Debug;
}
