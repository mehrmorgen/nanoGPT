Feature: Runtime high-level features
  As an operator
  I want the runtime CLI to handle missing resources gracefully
  So that I can debug configuration errors

  Scenario: Prepare with missing configuration file
    When I invoke the "ml-playground" CLI with arguments "--exp-config non_existent_config.toml prepare dummy"
    Then the command exits with code 2
    And the output contains:
      | text |
      | Config file not found |

  Scenario: Sample with missing configuration file
    When I invoke the "ml-playground" CLI with arguments "--exp-config non_existent_config.toml sample dummy"
    Then the command exits with code 2
    And the output contains:
      | text |
      | Config file not found |
