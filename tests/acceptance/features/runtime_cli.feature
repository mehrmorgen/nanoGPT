Feature: Runtime CLI entrypoint
  As an operator running experiments
  I want the runtime CLI help to describe core commands and options
  So that I know how to prepare, train, sample, or analyze experiments

  Scenario: Display runtime CLI help summary
    When I invoke the "ml-playground" CLI with arguments "--help"
    Then the command exits with code 0
    And the output contains:
      | text |
      | ML Playground CLI |
      | prepare |
      | train |
      | sample |
      | analyze |
      | --exp-config |

  Scenario: Display prepare command help
    When I invoke the "ml-playground" CLI with arguments "prepare --help"
    Then the command exits with code 0
    And the output contains:
      | text |
      | Prepare data for an experiment |

  Scenario: Display train command help
    When I invoke the "ml-playground" CLI with arguments "train --help"
    Then the command exits with code 0
    And the output contains:
      | text |
      | Train a model for an experiment |
