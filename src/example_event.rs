use rkyv::{Archive, Deserialize, Serialize};

#[derive(Archive, Deserialize, Serialize)]
pub struct Phys4Vec {
    energy: f64,
    px: f64,
    py: f64,
    pz: f64,
}

#[derive(Archive, Deserialize, Serialize)]
pub struct PhysicalParticle {
    z: u8,
    a: u8,
    momentum: Phys4Vec,
}

#[derive(Archive, Deserialize, Serialize)]
pub struct MeasuredParticle {
    si_energy: f64,
    csi_energy: f64,
    linearized_projection_val: f64,
    detector_idx: u16,
    phys_particle: PhysicalParticle,
}

#[derive(Archive, Deserialize, Serialize)]
pub struct MyEvent {
    particles: Vec<MeasuredParticle>,
    time: u64,
    pub trigger: u16,
}

pub struct Res {
    pub trigger: u16,
}
