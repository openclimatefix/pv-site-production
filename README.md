# PV Site Production
<!-- ALL-CONTRIBUTORS-BADGE:START - Do not remove or modify this section -->
[![All Contributors](https://img.shields.io/badge/all_contributors-1-orange.svg?style=flat-square)](#contributors-)
<!-- ALL-CONTRIBUTORS-BADGE:END -->

## Repo structure

[`./infrastructure`][inf]: Docker files for the services


At the core of each service is a self-contained python package. See the individual directories:

[`./forecast-inference`][infe]: Service that runs the site forecasting model in inference

[`./database-cleanup`][arc]: Service that archives the forecasts to keep the database small


## Development

### Run the CI for the whole repo

To make sure that everything builds, passes the lint and tests, you can run

    make all

Optionally run everything in parallel:

    make all -j 8


[inf]: ./infrastructure
[infe]: ./forecast-inference
[arc]: ./database-cleanup

## Contributors ✨

Thanks goes to these wonderful people ([emoji key](https://allcontributors.org/docs/en/emoji-key)):

<!-- ALL-CONTRIBUTORS-LIST:START - Do not remove or modify this section -->
<!-- prettier-ignore-start -->
<!-- markdownlint-disable -->
<table>
  <tbody>
    <tr>
      <td align="center" valign="top" width="14.28%"><a href="https://github.com/Sukh-P"><img src="https://avatars.githubusercontent.com/u/42407101?v=4?s=100" width="100px;" alt="Sukh-P"/><br /><sub><b>Sukh-P</b></sub></a><br /><a href="https://github.com/openclimatefix/pv-site-production/pulls?q=is%3Apr+reviewed-by%3ASukh-P" title="Reviewed Pull Requests">👀</a></td>
    </tr>
  </tbody>
</table>

<!-- markdownlint-restore -->
<!-- prettier-ignore-end -->

<!-- ALL-CONTRIBUTORS-LIST:END -->

This project follows the [all-contributors](https://github.com/all-contributors/all-contributors) specification. Contributions of any kind welcome!