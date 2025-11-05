/**
 * This algorithm is originally developed
 * by David Blackman and Sebastiano Vigna (vigna@acm.org)
 * http://xoroshiro.di.unimi.it/xoroshiro128plus.c
 *
 * And Tanabe Takayuki custmized.
 *
 * Additional methods for specific distributions (Uniform, Zipf) were integrated.
 */

#pragma once

#include <stdint.h>
#include <random>
#include <cmath>

class Xoroshiro128Plus
{
public:
    Xoroshiro128Plus(uint64_t seed = 0)
    {
        if (seed == 0)
        {
            std::random_device rd;
            s[0] = (static_cast<uint64_t>(rd()) << 32) | rd();
            s[1] = (static_cast<uint64_t>(rd()) << 32) | rd();
        }
        else
        {
            s[0] = splitMix64(seed);
            s[1] = splitMix64(s[0]);
        }
    }

    uint64_t s[2];

    // Generates the next 64-bit pseudo-random number.
    uint64_t next(void)
    {
        const uint64_t s0 = s[0];
        uint64_t s1 = s[1];
        const uint64_t result = s0 + s1;

        s1 ^= s0;
        s[0] = rotl(s0, 24) ^ s1 ^ (s1 << 16); // a, b
        s[1] = rotl(s1, 37);                   // c

        return result;
    }

    // Overload operator() to be used as a standard generator.
    uint64_t operator()() { return next(); }

    // ------------------- ここから追加するメソッド -------------------

    // Generates a random double in [0.0, 1.0).
    double NextUniform()
    {
        return (next() >> 11) * (1.0 / (1LL << 53));
    }

    // Generates a random integer uniformly distributed in [0, n-1].
    uint64_t Uniform(uint64_t n)
    {
        return next() % n;
    }

    // Generates a random integer from a Zipfian distribution in [0, n-1].
    uint64_t Zipf(uint64_t n, double theta)
    {
        if (!zipf_generator_initialized || n != zipf_n || theta != zipf_theta)
        {
            init_zipf_generator(n, theta);
        }

        double alpha = 1.0 / (1.0 - theta);
        double zetan = zipf_zeta_n;
        double eta = (1.0 - std::pow(2.0 / n, 1.0 - theta)) /
                     (1.0 - zipf_zeta_2 / zetan);

        double u = NextUniform();
        double uz = u * zetan;

        if (uz < 1.0)
            return 0;
        if (uz < 1.0 + std::pow(0.5, theta))
            return 1;

        return static_cast<uint64_t>(n * std::pow(eta * u - eta + 1.0, alpha));
    }

    // ------------------- 追加メソッドここまで -------------------

private:
    // For seeding the generator.
    uint64_t splitMix64(uint64_t seed)
    {
        uint64_t z = (seed += 0x9e3779b97f4a7c15);
        z = (z ^ (z >> 30)) * 0xbf58476d1ce4e5b9;
        z = (z ^ (z >> 27)) * 0x94d049bb133111eb;
        return z ^ (z >> 31);
    }

    inline uint64_t rotl(const uint64_t x, int k)
    {
        return (x << k) | (x >> (64 - k));
    }

    // --- Zipfian distribution helper variables and functions ---
    // mutable allows these to be modified even in const methods.
    mutable bool zipf_generator_initialized = false;
    mutable uint64_t zipf_n = 0;
    mutable double zipf_theta = 0.0;
    mutable double zipf_zeta_n = 0.0;
    mutable double zipf_zeta_2 = 0.0;

    // Pre-computes constants for the Zipfian distribution.
    void init_zipf_generator(uint64_t n, double theta) const
    {
        Xoroshiro128Plus *non_const_this = const_cast<Xoroshiro128Plus *>(this);
        non_const_this->zipf_zeta_n = 0;
        for (uint64_t i = 0; i < n; i++)
        {
            non_const_this->zipf_zeta_n += 1 / std::pow(i + 1, theta);
        }
        non_const_this->zipf_zeta_2 = 1 + (1 / std::pow(2, theta));
        non_const_this->zipf_n = n;
        non_const_this->zipf_theta = theta;
        non_const_this->zipf_generator_initialized = true;
    }
};
using Random = Xoroshiro128Plus;