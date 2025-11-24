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

  Scenario: Display quality tools help
    When I invoke the "tools" CLI with arguments "quality --help"
    Then the command exits with code 0
    And the output contains:
      | text |
      | Code quality tools |
      | lint |
      | format |
      | typecheck |

  Scenario: Display env tools help
    When I invoke the "tools" CLI with arguments "env --help"
    Then the command exits with code 0
    And the output contains:
      | text |
      | Environment management tools |
      | setup |
      | verify |

  Scenario: Display dev tools help
    When I invoke the "tools" CLI with arguments "dev --help"
    Then the command exits with code 0
    And the output contains:
      | text |
      | Development workflow tools |
      | review-list |
      | workflow-status |
