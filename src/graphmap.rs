use typedrw::TypedMemoryMap;

pub struct GraphMMap {
    nodes: TypedMemoryMap<u64>,
    edges: TypedMemoryMap<u32>,
}

impl GraphMMap {
    pub fn nodes(&self) -> usize { self.nodes[..].len() }
    pub fn edges(&self, node: usize) -> &[u32] {
        let nodes = &self.nodes[..];
        if node + 1 < nodes.len() {
            let start = nodes[node] as usize;
            let limit = nodes[node+1] as usize;
            &self.edges[..][start..limit]
        }
        else { &[] }
    }
    pub fn new(prefix: &str) -> GraphMMap {
        GraphMMap {
            nodes: TypedMemoryMap::new(format!("{}.offsets", prefix)),
            edges: TypedMemoryMap::new(format!("{}.targets", prefix)),
        }
    }
}
