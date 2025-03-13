import ccxt
import os
import logging
import sys
from typing import Dict, Optional, Tuple
from datetime import datetime
import time
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('arbitrage_bot.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

class Config:
    """Configuration class with environment variable support"""
    BINANCE_API_KEY = os.getenv('BINANCE_API_KEY')
    BINANCE_API_SECRET = os.getenv('BINANCE_API_SECRET')
    BYBIT_API_KEY = os.getenv('BYBIT_API_KEY')
    BYBIT_API_SECRET = os.getenv('BYBIT_API_SECRET')
    SYMBOL = os.getenv('ARBITRAGE_SYMBOL', 'BTC/USDT')
    AMOUNT = float(os.getenv('ARBITRAGE_AMOUNT', '0.01'))
    MIN_PROFIT_PERCENT = float(os.getenv('MIN_PROFIT_PERCENT', '0.1'))  # Minimum profit threshold
    RETRY_ATTEMPTS = int(os.getenv('RETRY_ATTEMPTS', '3'))
    SLEEP_INTERVAL = int(os.getenv('SLEEP_INTERVAL', '5'))  # Seconds between checks

    @classmethod
    def validate(cls):
        """Validate required configuration"""
        required = [
            cls.BINANCE_API_KEY, cls.BINANCE_API_SECRET,
            cls.BYBIT_API_KEY, cls.BYBIT_API_SECRET
        ]
        if any(not key for key in required):
            raise ValueError("All API keys and secrets must be set in environment variables")

class ArbitrageBot:
    def __init__(self):
        """Initialize the arbitrage bot"""
        self.config = Config()
        Config.validate()
        
        self.binance = ccxt.binance({
            'apiKey': self.config.BINANCE_API_KEY,
            'secret': self.config.BINANCE_API_SECRET,
            'enableRateLimit': True,
            'options': {'defaultType': 'spot'}
        })
        
        self.bybit = ccxt.bybit({
            'apiKey': self.config.BYBIT_API_KEY,
            'secret': self.config.BYBIT_API_SECRET,
            'enableRateLimit': True,
            'options': {'defaultType': 'spot'}
        })
        
        self.load_markets()

    def load_markets(self) -> None:
        """Load markets for both exchanges"""
        try:
            self.binance.load_markets()
            self.bybit.load_markets()
            if self.config.SYMBOL not in self.binance.markets or self.config.SYMBOL not in self.bybit.markets:
                raise ValueError(f"Symbol {self.config.SYMBOL} not available on one or both exchanges")
            logger.info(f"Markets loaded successfully for {self.config.SYMBOL}")
        except Exception as e:
            logger.error(f"Failed to load markets: {e}")
            sys.exit(1)

    @retry(
        stop=stop_after_attempt(Config.RETRY_ATTEMPTS),
        wait=wait_exponential(multiplier=1, min=4, max=10),
        retry=retry_if_exception_type(Exception)
    )
    def fetch_prices(self) -> Tuple[float, float]:
        """Fetch latest prices from both exchanges with retry logic"""
        try:
            binance_ticker = self.binance.fetch_ticker(self.config.SYMBOL)
            bybit_ticker = self.bybit.fetch_ticker(self.config.SYMBOL)
            return binance_ticker['last'], bybit_ticker['last']
        except Exception as e:
            logger.error(f"Price fetch failed: {e}")
            raise

    def check_balance(self) -> bool:
        """Check if sufficient balance exists on both exchanges"""
        try:
            binance_balance = self.binance.fetch_balance()
            bybit_balance = self.bybit.fetch_balance()
            
            base, quote = self.config.SYMBOL.split('/')
            min_amount = self.config.AMOUNT * 1.1  # 10% buffer for fees
            
            binance_base = float(binance_balance['total'].get(base, 0))
            binance_quote = float(binance_balance['total'].get(quote, 0))
            bybit_base = float(bybit_balance['total'].get(base, 0))
            bybit_quote = float(bybit_balance['total'].get(quote, 0))
            
            if binance_quote < min_amount or bybit_quote < min_amount:
                logger.error(f"Insufficient {quote} balance")
                return False
            if binance_base < self.config.AMOUNT or bybit_base < self.config.AMOUNT:
                logger.error(f"Insufficient {base} balance")
                return False
            return True
        except Exception as e:
            logger.error(f"Balance check failed: {e}")
            return False

    def execute_trade(self, buy_exchange: str, sell_exchange: str, 
                     buy_price: float, sell_price: float) -> bool:
        """Execute arbitrage trade"""
        try:
            profit_percent = abs((sell_price - buy_price) / buy_price * 100)
            if profit_percent < self.config.MIN_PROFIT_PERCENT:
                logger.info(f"Profit {profit_percent:.2f}% below threshold {self.config.MIN_PROFIT_PERCENT}%")
                return False

            if buy_exchange == 'binance':
                buy_order = self.binance.create_market_buy_order(self.config.SYMBOL, self.config.AMOUNT)
                sell_order = self.bybit.create_market_sell_order(self.config.SYMBOL, self.config.AMOUNT)
            else:
                buy_order = self.bybit.create_market_buy_order(self.config.SYMBOL, self.config.AMOUNT)
                sell_order = self.binance.create_market_sell_order(self.config.SYMBOL, self.config.AMOUNT)

            logger.info(f"Executed: Buy @ {buy_price:.2f} on {buy_exchange}, "
                       f"Sell @ {sell_price:.2f} on {sell_exchange}")
            logger.info(f"Profit: {profit_percent:.2f}%")
            return True
        except Exception as e:
            logger.error(f"Trade execution failed: {e}")
            return False

    def run(self) -> None:
        """Main execution loop"""
        logger.info(f"Arbitrage Bot started for {self.config.SYMBOL}")
        while True:
            try:
                if not self.check_balance():
                    logger.error("Insufficient balance, stopping")
                    time.sleep(self.config.SLEEP_INTERVAL)
                    continue

                binance_price, bybit_price = self.fetch_prices()
                logger.info(f"Binance: {binance_price:.2f} | Bybit: {bybit_price:.2f}")

                if binance_price > bybit_price:
                    logger.info("Opportunity: Buy Bybit, Sell Binance")
                    success = self.execute_trade('bybit', 'binance', bybit_price, binance_price)
                else:
                    logger.info("Opportunity: Buy Binance, Sell Bybit")
                    success = self.execute_trade('binance', 'bybit', binance_price, bybit_price)

                time.sleep(self.config.SLEEP_INTERVAL)

            except KeyboardInterrupt:
                logger.info("Bot stopped by user")
                break
            except Exception as e:
                logger.error(f"Main loop error: {e}")
                time.sleep(self.config.SLEEP_INTERVAL)

if __name__ == "__main__":
    try:
        bot = ArbitrageBot()
        bot.run()
    except ValueError as e:
        logger.error(f"Configuration error: {e}")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Startup error: {e}")
        sys.exit(1)
