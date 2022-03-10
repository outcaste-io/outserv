# Smart License: Frequently Asked Questions

### 1. Can you summarize what is allowed with the Smart License 1.0?

Smart License allows the right to use, modify, create derivative works, and
commercialize, with these limitations:

- You may not circumvent the monetization module.
- You may not remove or obscure any licensing, copyright, or other notices.

### 2. What is monetization module? What does it do?

Smart License follows an open pricing model. The pricing and the charging
mechanism for the software is baked into the software itself, in a module we
call the monetization module.

We are currently using a pricing model based on the number of cores used by the
program. The current charging method is via blockchain. In the future, we could
consider adding alternate ways of payment. You can read the source code of the
module [here](TODO: add link).

### 3. Is Smart License open source?

No. Smart License is not OSI approved license. But, we consider it to be under
"open ethos", a motto we identify with as allowing all the liberties of usage,
modification, derivation and commercialization, while also ensuring that the
developers of the software can make a living.

### 4. Can I provide a managed service using a project under Smart License?

Absolutely. As opposed to other such licenses (like Elastic or
SSPL), Smart License allows building commercial products, including building
managed services. In fact, we encourage others to do so, to make
this software as broadly available as possible for end users.

### 5. If I copy only certain parts of the code into another program, do I still need to pay?

Yes. If you copy any part of the code base under the Smart License,
your program should pay and do the equivalent of what the monetization module
would have done.

### 6. Why build this license? Why charge for this software?

We have build a lot of open source software in the past, with none of them
providing any source of income for the developers. Currently, there are three ways
to fund the development of open source.

a. Use your personal savings to fund the development. Or, have your company fund
    the development of the project.
b. Build open source part time while working for another company, if they allow that.
c. Take money from VCs. This generally results in building proprietary features and a cloud managed service to monetize the software.

Option a: If possible, this is the best way to go. But, might not be available as an
option for most developers.

Option b: For work-life balance, this option is highly unwieldy.

Option c: Once you take money from VCs, it becomes your fiduciary duty to
    optimize stock holder interests. To generate revenue, developers often mix
    open source (free) features with proprietary (paid) features (also known as
    open core model), which causes a rift for the developing team between how much do
    you "give away for free" and what to you charge for. Moreover, the
    proprietary features typically come with opaque pricing models for
    on-premises deployments.

Second and the most widely used revenue stream is to provide a cloud hosted
    solution. To maximize the revenue there and eliminate competing cloud
    services, the license is usually changed to a non open-source license which
    disallows others from building similar services.

We think none of these options are particularly attractive in a world where
users are increasingly willing to pay for the software, as long as the pricing
model is open and fair to all.

*With Smart License, we aim to hit the nail on its head.* Make the software charge
for its usage with an open pricing model baked into the code itself, available
and visible to everyone. And provide as much freedom as possible for others to
use, modify or build commercial services around the software.

> Not open source. Open ethos.

**TL;DR: A software under Smart License provides a direct income stream to the
developer so they can focus on the software, while eliminating the rift between
free v/s paid feature set, opaque pricing models or disallowing others from
building commercial services.**


### 7. How does Smart License compare against Elastic License?

Smart License is modeled after Elastic License 2.0, but differs in the limitations
it imposes.

Elastic License 2.0 has this limitation, that Smart License does NOT have:

*You may not provide the software to third parties as a hosted or managed service, where the service provides users with access to any substantial set of the features or functionality of the software.*

As mentioned above, we encourage managed service
providers to make this software as widely available as possible.

Elastic License 2.0 has this limitation, which Smart License has modified:

*EL2.0: You may not move, change, disable, or circumvent the license key
functionality in the software, and you may not remove or obscure any
functionality in the software that is protected by the license key.*

*SL1.0: The software contains a module that allows the licensor to monetize your use of
the software. You may not move, change, disable, or circumvent this functionality
in the software.*

You can see a full diff of Elastic 2.0 vs Smart 1.0 license here (TODO: add link).

