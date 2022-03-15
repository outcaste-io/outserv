# Monetization module

Billing module is the monetization module as mentioned in the Smart License
v1.0.

The current mechanism of charging for this software is based on CPU core usage.
We do not make any distinction between physical cores and hyper threads. The
core usage is tracked based on the **logical cores** available to the process. The
usage tracked should be similar to what `htop` (in Linux) would show for the
process.

The pricing is set based on core-hours used across the entire Outserv cluster.
**The current pricing is set to 3 USD cents/core-hour.** Every server would use a
minimum of 1 core-hour every hour.

It does not matter how many cores the process has access to. The usage is
charged based on how "busy" the cores were. For example, if the server had
access to 32 cores, but held an average usage of 4 cores in the hour (4
cores were busy the entire hour), then the server used 4 core-hours.

If the server was idle the entire hour, then the usage might be 0.05 core-hours.
But, given the minimum of 1 core-hour, usage would be set to 1 core-hour.

Similarly, if the server only had access to 2 cores, then the usage would always
be less than or equal to 2 core-hours.

Every server in the cluster tracks its own usage and sends that information for
accumulation across Zero modules. The Zero leader then charges every 15 days or
$100, whichever comes earlier. This mechanism might change in the future. It's
best to look at the code for a definitive answer.
