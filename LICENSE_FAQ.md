# Smart License: Frequently Asked Questions

### 1. Can you summarize what is allowed with the Smart License 1.0?

Smart License allows the right to use, modify, create derivative works, and
commercialize, with these limitations:

- You may not circumvent the monetization module.
- You may not remove or obscure any licensing, copyright, or other notices.

### 2. What is a monetization module? What does it do?

Smart License follows an open pricing model. The pricing and the charging
mechanism for the software are baked into the software itself, in a module we
call the monetization module.

You can read the source code of the module [here](/billing).

### 3. Is Smart License Open Source?

No. Smart License imposes limitations around modification of the monetization
module, which is against OSI guidelines.

### 4. Is Smart License Free Software?

No. Smart License imposes limitations around modification of the monetization
module, which is against FSF guidelines.

### 5. Is Smart License Proprietary?

Not quite. Most proprietary softwares do not come with the source code, or
encourage sharing and commercialization. Smart License allows the software to be
modified, redistributed and commercialized.

### 6. Is Smart License EULA?

No. EULAs do not allow users to change or distribute code.

### 7. Is Smart License Freeware?

Not quite. Freeware is software that is distributed at no monetary cost to the
end user. The source code is typically not made available. Smart License is the
opposite of freeware, where the source code is freely available to modify, but
comes at a monetary cost.

### 8. Is Smart License Shareware?

No. Shareware is a type of proprietary software which is initially shared by the
owner for trial use at little or no cost with usually limited functionality or
incomplete documentation but which can be upgraded upon payment. Code under
Smart License is expected to be provided with full functionality with no
upgrades beyond the enforced monetization.

### 9. What is Smart License?

**Smart License is a very liberal commercial license.** The only main limitation
Smart License adds is to ensure a source of income for the developing entity.
Beyond that, it provides all the liberties expected from a free and open source
software, which includes modifications, redistributions and commercialization,
without discrimination.

We think Open Source has taken hits recently with many open source companies
moving away from it to use non-open source licenses (SSPL, Elastic License,
various community licenses) to eliminate competing SaaS offerings. Smart License
provides an alternative solution which doesn't discriminate against
commercialization of the software by anyone.

There have been many instances where the software becomes popular, but the
developers behind the software are left high and dry, unable to balance their
financial needs against the time and effort it takes to maintain the software,
ultimately giving up on the software and moving on.

Smart License is a practical solution to that, while staying true to the many
liberties promised by FOSS. We believe users are willing to pay for the software
that benefits them. And by charging for the software, Smart License creates the
right alignment between the developers and the users. Hence, ensuring the
longevity of the software which is the best outcome for everyone.

### 10. How does Smart License compare against Elastic License?

Smart License 1.0 is modeled after [Elastic License
2.0](https://www.elastic.co/licensing/elastic-license) but differs in the
limitations it imposes. Elastic License 2.0 has this limitation, that Smart
License 1.0 does NOT have:

> ELv2: You may not provide the software to third parties as a hosted or managed
  service, where the service provides users with access to any substantial set of
  the features or functionality of the software.

As mentioned above, we encourage managed service providers to make this software
as widely available as possible. Elastic License 2.0 has this limitation, which
Smart License 1.0 has modified:

> ELv2: You may not move, change, disable, or circumvent the license key
  functionality in the software, and you may not remove or obscure any
  functionality in the software that is protected by the license key.

> SLv1: The software contains a module that allows the licensor to monetize your use of
  the software. You may not move, change, disable, or circumvent this functionality
  in the software.

### 11. Can I provide a managed service using a project under Smart License?

Absolutely. As opposed to other such licenses (like Elastic License or
SSPL), Smart License allows building commercial products, including building
managed services. In fact, we encourage others to do so, to make
this software as broadly available as possible for end-users.

### 12. If I fork the codebase, do I need to make my derivative work publicly available?

No. If you fork the codebase and make modifications, those changes can stay
private. You have no obligation to make that public. You can even commercialize
this fork by charging for the software (at a higher price than what you're
paying), linking it to your proprietary code, or creating proprietary services
around the fork.

However, the fork must continue to abide by the limitations of the Smart
License, which includes keeping the monetization module, licensing, copyright,
and other notices.

### 13. If I copy only certain parts of the code into another program, do I still need to pay?

Yes. If you copy any part of the codebase under the Smart License,
your program should pay and do the equivalent of what the monetization module
would have done.

If you use any of this software, you must keep and not circumvent the
monetization module. You can't use only other portions of the software.

### 14. Why build this license? Why charge for this software?

We have built a lot of open-source software in the past, with none of them
providing any source of income for the developers. Currently, there are three ways
to fund the development of open source.

1. Use your personal savings to fund the development. Or, have your company fund
    the development of the project.
2. Build open-source part-time while working for another company, if they allow that.
3. Take money from VCs. This generally results in building proprietary features
    and a cloud-managed service to monetize the software.

**Option 1:** If possible, this is the best way to go. But, might not be available as an
option for most developers.

**Option 2:** For work-life balance, this option is highly unwieldy.

**Option 3:** Once you take VC money, it becomes your fiduciary duty to
    optimize stockholder interests. To generate revenue, developers often mix
    open-source (free) features with proprietary (paid) features (also known as
    the open core model), which causes a rift for the developing team between
    how much do you "give away for free" and what do you charge for. Moreover,
    the proprietary features typically come with opaque pricing models for
    on-premises deployments.

The second and the most widely used revenue stream is to provide a cloud-hosted
    solution. To maximize the revenue there and eliminate competing cloud
    services, the license is usually changed to a non-open-source license which
    disallows others from building similar services.

We think none of these options are particularly attractive in a world where
users are increasingly willing to pay for the software, as long as the pricing
model is open and fair to all.

*With Smart License, we aim to hit the nail on its head.* Make the software charge
for its usage with an open pricing model baked into the code itself, available
and visible to everyone. And provide as much freedom as possible for others to
use, modify or build commercial services around the software, under our motto of
open ethos.

**TL;DR: A software under Smart License provides a direct income stream to the
developer so they can focus on the software while eliminating the rift between
free v/s paid feature set, opaque pricing models, or disallowing others from
building commercial services.**


### 15. Can I apply Smart License to my project?

Yes. We think more developers should be using Smart License to make a living out
of their hard work, while also staying as close to open source as possible.
That's the motto behind the "open ethos" concept.

The best application for Smart License is standalone servers. It can also be
applied to heavy-weight libraries, where the developer believes a payment for
usage is justified.
