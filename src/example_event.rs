use rkyv::{vec::ArchivedVec, Archive, Deserialize, Serialize};

use rand::{thread_rng, Rng};
use rkyv_impl::archive_impl;

#[derive(Archive, Deserialize, Serialize)]
pub struct Phys4Vec {
    energy: f64,
    px: f64,
    py: f64,
    pz: f64,
}

impl Phys4Vec {
    pub fn mag_3(&self) -> f64 {
        (self.px.powi(2) + self.py.powi(2) + self.pz.powi(2)).sqrt()
    }
}

impl ArchivedPhys4Vec {
    pub fn mag_3(&self) -> f64 {
        (self.px.powi(2) + self.py.powi(2) + self.pz.powi(2)).sqrt()
    }
}

impl Phys4Vec {
    pub fn random() -> Self {
        let mut rng = thread_rng();
        Self {
            energy: rng.gen_range(-100_f64..100_f64),
            px: rng.gen_range(-100_f64..100_f64),
            py: rng.gen_range(-100_f64..100_f64),
            pz: rng.gen_range(-100_f64..100_f64),
        }
    }
}

#[derive(Archive, Deserialize, Serialize)]
pub struct PhysicalParticle {
    z: u8,
    a: u8,
    momentum: Phys4Vec,
}

impl PhysicalParticle {
    pub fn random() -> Self {
        let mut rng = thread_rng();
        let z = rng.gen_range(0..18);
        Self {
            z,
            a: z + rng.gen_range(0..1),
            momentum: Phys4Vec::random(),
        }
    }
}

impl PhysicalParticle {
    pub fn momentum(&self) -> &Phys4Vec {
        &self.momentum
    }
}

impl ArchivedPhysicalParticle {
    pub fn momentum(&self) -> &ArchivedPhys4Vec {
        &self.momentum
    }
}

#[derive(Archive, Deserialize, Serialize)]
pub struct MeasuredParticle {
    si_energy: f64,
    csi_energy: f64,
    linearized_projection_val: f64,
    detector_idx: u16,
    phys_particle: PhysicalParticle,
}

impl MeasuredParticle {
    pub fn random() -> Self {
        let mut rng = thread_rng();
        Self {
            si_energy: rng.gen_range(0_f64..100_f64),
            csi_energy: rng.gen_range(0_f64..100_f64),
            linearized_projection_val: rng.gen_range(0_f64..100_f64),
            detector_idx: rng.gen_range(0..100),
            phys_particle: PhysicalParticle::random(),
        }
    }
}

impl MeasuredParticle {
    pub fn physical_particle(&self) -> &PhysicalParticle {
        &self.phys_particle
    }
}
impl ArchivedMeasuredParticle {
    pub fn physical_particle(&self) -> &ArchivedPhysicalParticle {
        &self.phys_particle
    }
}

#[derive(Archive, Deserialize, Serialize)]
pub struct MyEvent {
    particles: Vec<MeasuredParticle>,
    time: u64,
    trigger: u16,
}

impl MyEvent {
    pub fn random() -> Self {
        let mut rng = thread_rng();
        let num_particles = rng.gen_range(1..12);
        let particles = (0..num_particles)
            .map(|_| MeasuredParticle::random())
            .collect();
        Self {
            particles,
            time: rng.gen_range(0..100),
            trigger: rng.gen_range(0..100),
        }
    }
}

#[archive_impl]
impl MyEvent {
    pub fn trigger(&self) -> u16 {
        self.trigger
    }

    pub fn multiplicity(&self) -> usize {
        self.particles.len()
    }
}

impl MyEvent {
    pub fn particles(&self) -> &Vec<MeasuredParticle> {
        &self.particles
    }
}

impl ArchivedMyEvent {
    pub fn event(&self) -> MyEvent {
        self.deserialize(&mut rkyv::Infallible).unwrap()
    }
}

impl ArchivedMyEvent {
    pub fn particles(&self) -> &ArchivedVec<ArchivedMeasuredParticle> {
        &self.particles
    }
}
