# Flexible Travel Optimization

Seli-supervised clustering system for determining the best allocation strategy for a traveling visitation system.

### Fixed Dynamics
> "child"s are fixed agents
> "EI"s are traveling agents
> "child"s are assigned to one "EI" each
> "EI"s must travel to visit all "child"s in their roster

#### Constraints
> Each "EI" has a maximum capacity of their roster
> All "EI"s must take on "child"s to be visited
> We have no control over the "EI"s schedules, only that which is allocated to them
> Adjust existing system to an optimized system moving forward with no deallocation to pivot to optimized system

#### Goals
> Lower average traveling time for "EI"s when visiting all of their children in roster
> Lower average traveling distance for "EI"s when visiting all of their children in roster
> Do not disrupt existing systems in the name of optimization
> System used as suggestions to make data-informed decisions as opposed to data-driven decisions, because human factors need to be accounted for
