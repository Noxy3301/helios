#pragma once

#include <string>

#include "helios.pb.h"

// Flat binary packing for TxExecuteReadPlan responses ("HELIOSRP" format).
namespace flat_plan {

/**
 * @brief Pack a read-plan response into the flat payload format.
 *
 * This is destructive: each StepResult is released after packing so large
 * read-plan responses do not keep both protobuf rows and the flat payload
 * alive.
 */
void pack(Helios::Protocol::TxExecuteReadPlan::Response& r,
                      std::string& out);

}  // namespace flat_plan
