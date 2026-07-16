//! Simulated sensor value with a high-limit alarm state machine.

/// What happened to the alarm state after a simulation step.
#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub enum AlarmTransition {
    /// No change in alarm state.
    None,
    /// The value crossed above the high limit; the alarm was raised.
    Raised,
    /// The value returned to or below the high limit; the alarm was cleared.
    Cleared,
}

/// A sensor value driven by external deltas, with a single high-limit alarm.
pub struct Simulation {
    value: f64,
    high_limit: f64,
    alarm_active: bool,
}

impl Simulation {
    pub fn new(start: f64, high_limit: f64) -> Self {
        Self {
            value: start,
            high_limit,
            alarm_active: false,
        }
    }

    /// Advance the sensor by `delta` and report any alarm transition.
    pub fn step(&mut self, delta: f64) -> AlarmTransition {
        self.value += delta;
        if !self.alarm_active && self.value > self.high_limit {
            self.alarm_active = true;
            AlarmTransition::Raised
        } else if self.alarm_active && self.value <= self.high_limit {
            self.alarm_active = false;
            AlarmTransition::Cleared
        } else {
            AlarmTransition::None
        }
    }

    pub fn value(&self) -> f64 {
        self.value
    }

    pub fn high_limit(&self) -> f64 {
        self.high_limit
    }

    pub fn alarm_active(&self) -> bool {
        self.alarm_active
    }

    /// Alarm severity scaled by excursion above the limit:
    /// 500 at the limit, +50 per unit above, capped at 1000.
    pub fn severity(&self) -> u16 {
        let excess = (self.value - self.high_limit).max(0.0);
        (500.0 + excess * 50.0).min(1000.0) as u16
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn raises_once_when_crossing_high_limit() {
        let mut sim = Simulation::new(9.5, 10.0);
        assert_eq!(sim.step(1.0), AlarmTransition::Raised); // 10.5 > 10.0
        assert!(sim.alarm_active());
        assert_eq!(sim.step(1.0), AlarmTransition::None); // still active, no re-raise
    }

    #[test]
    fn clears_once_when_returning_below_limit() {
        let mut sim = Simulation::new(9.5, 10.0);
        sim.step(1.0); // raised at 10.5
        assert_eq!(sim.step(-1.0), AlarmTransition::Cleared); // 9.5 <= 10.0
        assert!(!sim.alarm_active());
        assert_eq!(sim.step(-1.0), AlarmTransition::None);
    }

    #[test]
    fn no_alarm_while_below_limit() {
        let mut sim = Simulation::new(0.0, 10.0);
        assert_eq!(sim.step(1.0), AlarmTransition::None);
        assert!(!sim.alarm_active());
    }

    #[test]
    fn severity_scales_with_excursion_and_caps_at_1000() {
        let mut sim = Simulation::new(10.0, 10.0);
        sim.step(0.5); // value 10.5, excess 0.5
        assert_eq!(sim.severity(), 525); // 500 + 0.5 * 50

        let mut sim2 = Simulation::new(0.0, 10.0);
        sim2.step(100.0); // far above the limit
        assert_eq!(sim2.severity(), 1000);
    }
}
