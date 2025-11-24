Feature: Tools CLI entrypoint
  As a developer using the tools CLI
  I want core commands to expose helpful output and validation
  So that I can discover tooling tasks and configuration quickly

  Scenario: Display tools CLI help summary
    When I invoke the "tools" CLI with arguments "--help"
    Then the command exits with code 0
    And the output contains:
      | text |
      | ML Playground unified development tools |
      | quality |
      | test |
      | env |
      | dev |
      | ci |
      | learn |

  Scenario: Show effective tools configuration
    When I invoke the "tools" CLI with arguments "config"
    Then the command exits with code 0
    And the output contains:
      | text |
      | Current tools configuration |
      | Quality tools |
      | Testing tools |

  Scenario: Report unknown subcommands
    When I invoke the "tools" CLI with arguments "unknown-command"
    Then the command exits with code 2
    And the output contains:
      | text |
      | No such command |

  Scenario Outline: Display subcommand help
    When I invoke the "tools" CLI with arguments "<subcommand> --help"
    Then the command exits with code 0
    And the output contains:
      | text |
      | <expected_text> |

    Examples:
      | subcommand | expected_text |
      | quality    | Code quality tools |
      | test       | Testing tools |
      | env        | Environment management tools |
      | dev        | Development workflow tools |
      | ci         | CI/CD operations |
      | learn      | Learning mode utilities |

  Scenario Outline: Require subcommand selection
    When I invoke the "tools" CLI with arguments "<subcommand>"
    Then the command exits with code 2
    And the output contains:
      | text |
      | Usage: tools |

    Examples:
      | subcommand |
      | quality |
      | test |
      | env |
      | dev |
      | ci |
      | learn |
