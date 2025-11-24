Feature: Tools high-level features
  As a developer
  I want to use the tools CLI to manage my environment and configuration
  So that I can maintain a healthy development setup

  Scenario: Verify environment health
    When I invoke the "tools" CLI with arguments "env verify"
    Then the command exits with code 0
    And the output contains:
      | text |
      | ml_playground import OK |

  Scenario: Display tool version
    When I invoke the "tools" CLI with arguments "version"
    Then the command exits with code 0
    And the output contains:
      | text |
      | ML Playground Tools |
