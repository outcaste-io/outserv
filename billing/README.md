# Monetization

This is the monetization module mentioned in the Smart License.

## Pricing

The current pricing model for this software is based on Logical CPU usage. We
define a logical CPU as any schedulable entity. So every core/thread in a
multicore/thread processor is a logical CPU (henceforth just referred to as
CPU). The usage tracked should be similar to what `htop` (in Linux) would show
for the process.

The pricing is set based on cpu-hours used across the entire Outserv cluster.
Similar to kilowatt-hour, one cpu-hour is equal to one CPU usage sustained for
one hour. **The current pricing is set to 7¢ US (7 pennies) per cpu-hour.**
Every server would use a minimum of 2 cpu-hours every hour.

## Usage Tracking

It does not matter how many CPUs the process has access to. The usage is charged
based on how "busy" the CPUs were. For example, if the server had access to 32
CPUs, but held an sustained usage of 4 CPUs in the hour (4 CPUs were busy the
entire hour), then the server used 4 cpu-hours.

If the server was idle the entire hour, then the usage might be 0.05 cpu-hours.
But, given the minimum of 2 cpu-hours, usage would be accounted as 2 cpu-hours.

A user can restrict usage by limiting how many CPUs does the process have
access to. If the server only has access to 4 CPUs, then the usage would always
be between 2 to 4 cpu-hours per hour.

## Mechanism

Every server in the cluster tracks its own usage and sends that information for
accumulation via Zero. The Zero leader charges periodically. This mechanism
might change in the future. It's best to look at the code for a definitive
answer.
