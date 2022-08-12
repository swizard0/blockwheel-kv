pub trait Context {
    type Info;
    type Insert;
    type Lookup;
    type Remove;
    type Flush;
}
