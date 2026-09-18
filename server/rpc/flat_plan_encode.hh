#pragma once

#include <string>

#include "helios.pb.h"

// Flat binary encoder for TxExecuteReadPlan responses ("HELIOSRP" format).
namespace flat_plan {

/**
 * @brief Encode a read-plan response into the flat payload format.
 *
 * This is destructive: each StepResult is released after encoding so large
 * read-plan responses do not keep both protobuf rows and the flat payload
 * alive.
 */
void encode_to_string(Helios::Protocol::TxExecuteReadPlan::Response& r,
                      std::string& out);

}  // namespace flat_plan
