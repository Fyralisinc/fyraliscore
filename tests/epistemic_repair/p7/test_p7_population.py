from lib.evaluation.epistemic_repair.p7_population import build_p7_population


def test_all_optional_worlds_are_sealed_before_execution() -> None:
    population = build_p7_population()
    assert population.initial_world_count == 3
    assert population.maximum_world_count == len(population.worlds) == 7
    assert len(set(population.seeds)) == 7
    assert all(len(world.theses) == 4 and world.batch_count == 12 for world in population.worlds)
