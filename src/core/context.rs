pub trait Context {
    type Insert;
    type Lookup;
    type Remove;
    type Flush;
}
