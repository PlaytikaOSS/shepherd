package com.playtika.shepherd;

import com.playtika.shepherd.serde.SerDe;
import com.playtika.shepherd.serde.SerDeUtils;

import java.util.Objects;

public class JsonPushHerdTest extends AbstractPushHerdTest<JsonPushHerdTest.BlackSheep>{

    @Override
    protected SerDe<BlackSheep> getSerDe() {
        return SerDeUtils.getSerDe(BlackSheep.class);
    }

    @Override
    BlackSheep[] getPopulation0() {
        return new BlackSheep[0];
    }

    @Override
    BlackSheep[] getPopulation1() {
        return new BlackSheep[]{new BlackSheep("1", 1)};
    }

    @Override
    BlackSheep[] getPopulation2() {
        return new BlackSheep[]{new BlackSheep("123", 123)};
    }

    static class BlackSheep {
        private String name;
        private double weight;

        public BlackSheep() {
        }

        public BlackSheep(String name, double weight) {
            this.name = name;
            this.weight = weight;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public double getWeight() {
            return weight;
        }

        public void setWeight(double weight) {
            this.weight = weight;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            BlackSheep that = (BlackSheep) o;
            return Double.compare(that.weight, weight) == 0 && Objects.equals(name, that.name);
        }

        @Override
        public int hashCode() {
            return Objects.hash(name, weight);
        }

        @Override
        public String toString() {
            return "BlackSheep{" +
                    "name='" + name + '\'' +
                    ", weight=" + weight +
                    '}';
        }
    }
}
