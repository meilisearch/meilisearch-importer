use std::io::{self, Write};

pub struct ByteCount(usize);

impl ByteCount {
    pub fn new() -> Self {
        ByteCount(0)
    }

    pub fn count(&self) -> usize {
        self.0
    }
}

impl Write for ByteCount {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.0 += buf.len();
        Ok(buf.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}
