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

  Scenario Outline: Display runtime subcommand help
    When I invoke the "ml-playground" CLI with arguments "<subcommand> --help"
    Then the command exits with code 0
    And the output contains:
      | text |
      | <expected_text> |

    Examples:
      | subcommand | expected_text |
      | prepare    | Prepare data for an experiment |
      | train      | Train a model for an experiment |
      | sample     | Sample from a trained model |
      | analyze    | Run analysis for an experiment |

  Scenario Outline: Report missing arguments
    When I invoke the "ml-playground" CLI with arguments "<subcommand>"
    Then the command exits with code 2
    And the output contains:
      | text |
      | Missing argument |

    Examples:
      | subcommand |
      | prepare |
      | train |
      | sample |
      | analyze |
