use std::{collections::HashMap, sync::Arc};

use anyhow::Result;

pub struct Ring {
    id: i32,
    nodes: Vec<(String, i32)>,
    r_nodes: HashMap<i32, String>
}

impl Ring {
    pub fn new(node: String, raw_nodes: Vec<String>) -> Result<Arc<Self>> {
        let _id = id(&node)?;

        let mut nodes = Vec::new();
        let mut r_nodes = HashMap::new();
        for p in raw_nodes {
            let p_id = id(&p)?;
            nodes.push((p.clone(), p_id));
            r_nodes.insert(p_id, p);
        }

        Ok(Arc::new(Ring { id: _id, nodes, r_nodes }))
    }

    pub fn fancy_iter(&self) -> impl Iterator<Item = (&String, i32)> {
        let idx = self
            .nodes
            .iter()
            .position(|&(_, v)| v == self.id)
            .expect("Value not found in the Object's data");

        let first_part = &self.nodes[idx + 1..];
        let second_part = &self.nodes[..idx];
        first_part
            .iter()
            .chain(second_part.iter())
            .map(|(a, b)| (a, *b as i32))
    }

    pub fn get_by_id(&self, id: i32) -> Option<String> {
        self.r_nodes.get(&id).cloned()
    }
}

pub fn id(p: &str) -> anyhow::Result<i32> {
    Ok(p.split(".").nth(3).ok_or_else(|| anyhow::anyhow!("Wrong `p` format!"))?.parse::<i32>()?)
}
