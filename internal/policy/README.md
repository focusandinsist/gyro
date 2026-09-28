# Internal Failure Policies

This package owns concrete `gyro.FailurePolicy` implementations. Policies
consume route candidates and health observations; they never probe nodes or
manage resources.
