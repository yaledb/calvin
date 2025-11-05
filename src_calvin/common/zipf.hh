#pragma once

#include <cassert>
#include <cfloat>
#include <cmath>
#include <iostream>
#include <random>
#include <vector>

#include "inline.hh"
#include "random.hh"

using std::cout;
using std::endl;

// Fast zipf distribution by Jim Gray et al.
class FastZipf {
  Xoroshiro128Plus* rnd_;
  const size_t nr_;
  const double alpha_, zetan_, eta_;
  const double threshold_;

 public:
  FastZipf(Xoroshiro128Plus* rnd, double theta, size_t nr)
      : rnd_(rnd),
        nr_(nr),
        alpha_(1.0 / (1.0 - theta)),
        zetan_(zeta(nr, theta)),
        eta_((1.0 - std::pow(2.0 / (double)nr, 1.0 - theta)) /
             (1.0 - zeta(2, theta) / zetan_)),
        threshold_(1.0 + std::pow(0.5, theta)) {
    assert(0.0 <= theta);
    assert(theta < 1.0);  // 1.0 can not be specified.
  }

  // Use this constructor if zeta is pre-calculated.
  FastZipf(Xoroshiro128Plus* rnd, double theta, size_t nr, double zetan)
      : rnd_(rnd),
        nr_(nr),
        alpha_(1.0 / (1.0 - theta)),
        zetan_(zetan),
        eta_((1.0 - std::pow(2.0 / (double)nr, 1.0 - theta)) /
             (1.0 - zeta(2, theta) / zetan_)),
        threshold_(1.0 + std::pow(0.5, theta)) {
    assert(0.0 <= theta);
    assert(theta < 1.0);  // 1.0 can not be specified.
  }

  INLINE size_t operator()() {
    double u = rnd_->next() / (double)UINT64_MAX;
    double uz = u * zetan_;
    if (uz < 1.0)
      return 0;
    if (uz < threshold_)
      return 1;
    return (size_t)((double)nr_ * std::pow(eta_ * u - eta_ + 1.0, alpha_));
  }

  uint64_t rand() { return rnd_->next(); }

  static double zeta(size_t nr, double theta) {
    double ans = 0.0;
    for (size_t i = 0; i < nr; ++i)
      ans += std::pow(1.0 / (double)(i + 1), theta);
    return ans;
  }
};
