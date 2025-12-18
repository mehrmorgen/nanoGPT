"""Tests for vier_gewinnt player implementations."""

from __future__ import annotations

import pytest

from ml_playground.experiments.vier_gewinnt.engine import VierGewinnt
from ml_playground.experiments.vier_gewinnt.players import (
    RandomPlayer,
    HeuristicPlayer,
    MinimaxPlayer,
)


class TestRandomPlayer:
    def test_get_move_returns_valid_column(self):
        """Test that RandomPlayer always returns a valid move."""
        player = RandomPlayer()
        game = VierGewinnt()

        # Test multiple times due to randomness
        for _ in range(100):
            move = player.get_move(game)
            assert move in game.get_valid_moves()

    def test_get_move_fails_when_board_full(self):
        """Test RandomPlayer behavior on full board."""
        player = RandomPlayer()
        game = VierGewinnt()

        # Fill the board
        for col in range(7):
            for _ in range(6):
                game.make_move(col)

        with pytest.raises(IndexError):
            player.get_move(game)


class TestHeuristicPlayer:
    def test_winning_move_priority(self):
        """Test that HeuristicPlayer takes winning moves."""
        player = HeuristicPlayer()
        game = VierGewinnt()

        # Set up board where player 1 can win
        # Create a horizontal 3-in-a-row with space to complete
        game.make_move(3)  # Player 1
        game.make_move(0)  # Player 2
        game.make_move(3)  # Player 1
        game.make_move(0)  # Player 2
        game.make_move(3)  # Player 1
        game.make_move(0)  # Player 2
        game.current_player = 1  # Back to player 1

        move = player.get_move(game)
        assert move == 3  # Should complete the 4-in-a-row

    def test_blocking_move_priority(self):
        """Test that HeuristicPlayer blocks opponent wins."""
        player = HeuristicPlayer()
        game = VierGewinnt()

        # Set up board where only opponent (player 2) has a winning threat at column 3
        # Player 1 has no winning moves
        game.make_move(0)  # Player 1
        game.make_move(3)  # Player 2
        game.make_move(1)  # Player 1
        game.make_move(3)  # Player 2
        game.make_move(2)  # Player 1
        game.make_move(3)  # Player 2 - has 3 in column 3
        # Now it's player 1's turn and player 2 can win at column 3
        # Player 1 has no 3-in-a-row anywhere

        move = player.get_move(game)
        assert move == 3  # Should block the win


class TestMinimaxPlayer:
    def test_initialization(self):
        """Test MinimaxPlayer initialization."""
        player = MinimaxPlayer(depth=3)
        assert player.depth == 3
        assert player.player_id is None

    def test_get_move_returns_valid_column(self):
        """Test that MinimaxPlayer returns valid moves."""
        player = MinimaxPlayer(depth=2)  # Shallow for speed
        game = VierGewinnt()

        move = player.get_move(game)
        assert move in game.get_valid_moves()

    def test_depth_parameter_affects_performance(self):
        """Test that higher depth takes longer (basic sanity check)."""
        import time

        game = VierGewinnt()
        # Make some moves to create a non-trivial position
        for col in [3, 2, 4, 1]:
            game.make_move(col)

        # Time depth=2
        player2 = MinimaxPlayer(depth=2)
        start = time.time()
        player2.get_move(game)
        time2 = time.time() - start

        # Time depth=3
        player3 = MinimaxPlayer(depth=3)
        start = time.time()
        player3.get_move(game)
        time3 = time.time() - start

        # Depth 3 should take longer than depth 2
        assert time3 > time2

    def test_evaluate_board_terminal_states(self):
        """Test board evaluation for terminal states."""
        player = MinimaxPlayer(depth=1)
        player.player_id = 1

        # Test winning position - player 1 wins in column 0
        game = VierGewinnt()
        # Set up a win for player 1
        game.make_move(0)  # Player 1
        game.make_move(1)  # Player 2
        game.make_move(0)  # Player 1
        game.make_move(1)  # Player 2
        game.make_move(0)  # Player 1
        game.make_move(1)  # Player 2
        game.make_move(0)  # Player 1 wins

        score = player.evaluate_board(game)
        assert score == 100000

        # Test losing position - player 2 wins in column 0
        game = VierGewinnt()
        player.player_id = 1
        # Set up a win for player 2 by having player 1 start first in column 1
        game.make_move(1)  # Player 1 plays in column 1
        game.make_move(0)  # Player 2 plays in column 0
        game.make_move(1)  # Player 1 plays in column 1
        game.make_move(0)  # Player 2 plays in column 0
        game.make_move(1)  # Player 1 plays in column 1
        game.make_move(0)  # Player 2 plays in column 0
        game.make_move(2)  # Player 1 plays elsewhere to avoid winning
        game.make_move(0)  # Player 2 plays in column 0 and wins

        score = player.evaluate_board(game)
        assert score == -100000

    def test_alpha_beta_pruning(self):
        """Test that alpha-beta pruning reduces evaluations."""
        # This is a regression test to ensure pruning is working
        # We can't directly measure pruning, but we can ensure
        # the algorithm still finds good moves quickly
        player = MinimaxPlayer(depth=4)
        game = VierGewinnt()

        # Should complete in reasonable time (< 1 second)
        import time

        start = time.time()
        move = player.get_move(game)
        elapsed = time.time() - start

        assert elapsed < 1.0
        assert move in game.get_valid_moves()


class TestPlayerPerformance:
    """Performance regression tests for player optimizations."""

    def test_minimax_performance_threshold(self):
        """Test that MinimaxPlayer meets performance requirements."""
        import time

        player = MinimaxPlayer(depth=3)  # Optimized depth
        game = VierGewinnt()

        # Create a mid-game position
        for col in [3, 2, 4, 1, 5]:
            game.make_move(col)

        # Should make decision in under 100ms
        start = time.time()
        move = player.get_move(game)
        elapsed = time.time() - start

        assert elapsed < 0.1  # 100ms threshold
        assert move in game.get_valid_moves()

    def test_data_generation_speed(self):
        """Test that data generation is acceptably fast."""
        from ml_playground.experiments.vier_gewinnt.data_generator import play_game

        player1 = HeuristicPlayer()
        player2 = MinimaxPlayer(depth=3)

        import time

        start = time.time()

        # Play 10 games
        for _ in range(10):
            winner, moves = play_game(player1, player2)
            assert 0 <= winner <= 2
            assert len(moves) <= 42

        elapsed = time.time() - start
        avg_time = elapsed / 10

        # Should average under 1 second per game
        assert avg_time < 1.0
