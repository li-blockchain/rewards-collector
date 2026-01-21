import os
from web3 import Web3
from decimal import Decimal
import json
from typing import Dict, Any, Optional
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Aave V3 Pool contract addresses (Ethereum mainnet)
AAVE_POOL_ADDRESS = "0x87870Bca3F3fD6335C3F4ce8392D69350B4fA4E2"
AAVE_POOL_DATA_PROVIDER_ADDRESS = "0x7B4EB56E7CD4b454BA8ff71E4518426369a138a3"
AAVE_ORACLE_ADDRESS = "0x54586bE62E3c3580375aE3723C145253060Ca0C2"

# Token addresses (Ethereum mainnet)
WETH_ADDRESS = "0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2"
GHO_ADDRESS = "0x40D16FC0246aD3160Ccc09B8D0D3A2cD28aE6C2f"

# ABI for basic ERC20 functions
ERC20_ABI = [
    {
        "constant": True,
        "inputs": [{"name": "_owner", "type": "address"}],
        "name": "balanceOf",
        "outputs": [{"name": "balance", "type": "uint256"}],
        "type": "function"
    },
    {
        "constant": True,
        "inputs": [],
        "name": "decimals",
        "outputs": [{"name": "", "type": "uint8"}],
        "type": "function"
    },
    {
        "constant": True,
        "inputs": [],
        "name": "symbol",
        "outputs": [{"name": "", "type": "string"}],
        "type": "function"
    }
]

# ABI for Aave Pool - getUserAccountData method
POOL_ABI = [
    {
        "inputs": [{"internalType": "address", "name": "user", "type": "address"}],
        "name": "getUserAccountData",
        "outputs": [
            {"internalType": "uint256", "name": "totalCollateralBase", "type": "uint256"},
            {"internalType": "uint256", "name": "totalDebtBase", "type": "uint256"},
            {"internalType": "uint256", "name": "availableBorrowsBase", "type": "uint256"},
            {"internalType": "uint256", "name": "currentLiquidationThreshold", "type": "uint256"},
            {"internalType": "uint256", "name": "ltv", "type": "uint256"},
            {"internalType": "uint256", "name": "healthFactor", "type": "uint256"}
        ],
        "stateMutability": "view",
        "type": "function"
    }
]

# ABI for Aave Pool Data Provider - getUserReserveData method (for specific asset details)
POOL_DATA_PROVIDER_ABI = [
    {
        "inputs": [
            {"internalType": "address", "name": "asset", "type": "address"},
            {"internalType": "address", "name": "user", "type": "address"}
        ],
        "name": "getUserReserveData",
        "outputs": [
            {"internalType": "uint256", "name": "currentATokenBalance", "type": "uint256"},
            {"internalType": "uint256", "name": "currentStableDebt", "type": "uint256"},
            {"internalType": "uint256", "name": "currentVariableDebt", "type": "uint256"},
            {"internalType": "uint256", "name": "principalStableDebt", "type": "uint256"},
            {"internalType": "uint256", "name": "scaledVariableDebt", "type": "uint256"},
            {"internalType": "uint256", "name": "stableBorrowRate", "type": "uint256"},
            {"internalType": "uint256", "name": "liquidityRate", "type": "uint256"},
            {"internalType": "uint40", "name": "stableRateLastUpdated", "type": "uint40"},
            {"internalType": "bool", "name": "usageAsCollateralEnabled", "type": "bool"}
        ],
        "stateMutability": "view",
        "type": "function"
    }
]

# ABI for Aave Oracle
ORACLE_ABI = [
    {
        "inputs": [{"internalType": "address", "name": "asset", "type": "address"}],
        "name": "getAssetPrice",
        "outputs": [{"internalType": "uint256", "name": "", "type": "uint256"}],
        "stateMutability": "view",
        "type": "function"
    }
]

class CDPMonitor:
    def __init__(self):
        # Initialize Web3 connection to local node
        # Support multiple connection types: HTTP, WebSocket, IPC
        rpc_url = os.getenv('RPC_URL', 'http://localhost:8545')
        
        if rpc_url.startswith('ws://') or rpc_url.startswith('wss://'):
            # WebSocket connection
            self.w3 = Web3(Web3.WebsocketProvider(rpc_url))
        elif rpc_url.startswith('ipc://') or rpc_url.endswith('.ipc'):
            # IPC connection (Unix domain socket)
            self.w3 = Web3(Web3.IPCProvider(rpc_url.replace('ipc://', '')))
        else:
            # HTTP connection (default)
            self.w3 = Web3(Web3.HTTPProvider(rpc_url))
        
        # Test connection
        if not self.w3.is_connected():
            raise ConnectionError(f"Failed to connect to Ethereum node at {rpc_url}")
        
        print(f"Connected to Ethereum node: {self.w3.eth.chain_id}")
        
        # Initialize contracts
        self.pool = self.w3.eth.contract(
            address=AAVE_POOL_ADDRESS,
            abi=POOL_ABI
        )
        self.pool_data_provider = self.w3.eth.contract(
            address=AAVE_POOL_DATA_PROVIDER_ADDRESS,
            abi=POOL_DATA_PROVIDER_ABI
        )
        self.oracle = self.w3.eth.contract(
            address=AAVE_ORACLE_ADDRESS,
            abi=ORACLE_ABI
        )
        self.weth_contract = self.w3.eth.contract(
            address=WETH_ADDRESS,
            abi=ERC20_ABI
        )
        self.gho_contract = self.w3.eth.contract(
            address=GHO_ADDRESS,
            abi=ERC20_ABI
        )
        
        # Position address (should be set in environment variables)
        self.position_address = os.getenv('CDP_POSITION_ADDRESS')
        if not self.position_address:
            raise ValueError("CDP_POSITION_ADDRESS environment variable not set")
        
        # Health threshold (liquidation threshold + buffer)
        self.health_threshold = float(os.getenv('CDP_HEALTH_THRESHOLD', '1.5'))  # 1.5 = 150% collateralization
        
    def get_token_balance(self, token_contract, address: str) -> Decimal:
        """Get token balance for a given address"""
        try:
            balance = token_contract.functions.balanceOf(address).call()
            decimals = token_contract.functions.decimals().call()
            return Decimal(balance) / Decimal(10 ** decimals)
        except Exception as e:
            print(f"Error getting token balance: {e}")
            return Decimal(0)
    
    def get_token_price(self, token_address: str) -> Decimal:
        """Get token price from Aave Oracle"""
        try:
            price = self.oracle.functions.getAssetPrice(token_address).call()
            return Decimal(price) / Decimal(10 ** 8)  # Oracle returns price with 8 decimals
        except Exception as e:
            print(f"Error getting token price: {e}")
            return Decimal(0)
    
    def get_user_account_data(self) -> Dict[str, Any]:
        """Get user account data from Aave Pool"""
        try:
            account_data = self.pool.functions.getUserAccountData(self.position_address).call()
            
            # Aave uses 8 decimals for base values
            total_collateral_base = Decimal(account_data[0]) / Decimal(10 ** 8)
            total_debt_base = Decimal(account_data[1]) / Decimal(10 ** 8)
            available_borrows_base = Decimal(account_data[2]) / Decimal(10 ** 8)
            current_liquidation_threshold = Decimal(account_data[3]) / Decimal(10000)  # Basis points
            ltv = Decimal(account_data[4]) / Decimal(10000)  # Basis points
            health_factor = Decimal(account_data[5]) / Decimal(10 ** 18)  # 18 decimals
            
            return {
                'total_collateral_base': float(total_collateral_base),
                'total_debt_base': float(total_debt_base),
                'available_borrows_base': float(available_borrows_base),
                'current_liquidation_threshold': float(current_liquidation_threshold),
                'ltv': float(ltv),
                'health_factor': float(health_factor)
            }
        except Exception as e:
            print(f"Error getting user account data: {e}")
            return {
                'error': str(e)
            }
    
    def get_asset_specific_data(self) -> Dict[str, Any]:
        """Get specific asset data (WETH and GHO) - only if user has activity"""
        try:
            # Try to get WETH data
            try:
                weth_data = self.pool_data_provider.functions.getUserReserveData(
                    WETH_ADDRESS, self.position_address
                ).call()
                weth_supplied = Decimal(weth_data[0]) / Decimal(10 ** 18)
            except Exception:
                weth_supplied = Decimal(0)
            
            # Try to get GHO data
            try:
                gho_data = self.pool_data_provider.functions.getUserReserveData(
                    GHO_ADDRESS, self.position_address
                ).call()
                gho_borrowed = Decimal(gho_data[2]) / Decimal(10 ** 18)  # Variable debt
            except Exception:
                gho_borrowed = Decimal(0)
            
            return {
                'weth_supplied': float(weth_supplied),
                'gho_borrowed': float(gho_borrowed)
            }
        except Exception as e:
            print(f"Error getting asset specific data: {e}")
            return {
                'weth_supplied': 0.0,
                'gho_borrowed': 0.0
            }
    
    def get_position_data(self) -> Dict[str, Any]:
        """Get comprehensive position data"""
        try:
            # Get overall account data (this is the reliable method)
            account_data = self.get_user_account_data()
            
            if 'error' in account_data:
                return account_data
            
            # Get specific asset data (if available)
            asset_data = self.get_asset_specific_data()
            
            # Get current balances
            weth_balance = self.get_token_balance(self.weth_contract, self.position_address)
            gho_balance = self.get_token_balance(self.gho_contract, self.position_address)
            
            # Get prices
            weth_price = self.get_token_price(WETH_ADDRESS)
            gho_price = self.get_token_price(GHO_ADDRESS)
            
            # Calculate additional metrics
            total_collateral_value = account_data['total_collateral_base']
            total_debt_value = account_data['total_debt_base']
            
            if total_debt_value > 0:
                collateralization_ratio = total_collateral_value / total_debt_value
            else:
                collateralization_ratio = float('inf')
            
            return {
                'weth_supplied': asset_data['weth_supplied'],
                'weth_balance': float(weth_balance),
                'gho_borrowed': asset_data['gho_borrowed'],
                'gho_balance': float(gho_balance),
                'weth_price': float(weth_price),
                'gho_price': float(gho_price),
                'total_collateral_value': total_collateral_value,
                'total_debt_value': total_debt_value,
                'available_borrows': account_data['available_borrows_base'],
                'liquidation_threshold': account_data['current_liquidation_threshold'],
                'ltv': account_data['ltv'],
                'collateralization_ratio': collateralization_ratio,
                'health_factor': account_data['health_factor'],
                'is_healthy': account_data['health_factor'] >= self.health_threshold
            }
            
        except Exception as e:
            print(f"Error getting position data: {e}")
            return {
                'error': str(e),
                'is_healthy': False
            }
    
    def generate_position_report(self) -> str:
        """Generate a human-readable position report"""
        data = self.get_position_data()
        
        if 'error' in data:
            return f"❌ Error getting position data: {data['error']}"
        
        report = "📊 **CDP Position Report**\n\n"
        
        # Account Overview
        report += f"**Account Overview:**\n"
        report += f"  • Total Collateral: ${data['total_collateral_value']:.2f}\n"
        report += f"  • Total Debt: ${data['total_debt_value']:.2f}\n"
        report += f"  • Available to Borrow: ${data['available_borrows']:.2f}\n"
        report += f"  • LTV: {data['ltv']:.2f}%\n"
        report += f"  • Liquidation Threshold: {data['liquidation_threshold']:.2f}%\n\n"
        
        # Asset Details (if available)
        if data['weth_supplied'] > 0 or data['gho_borrowed'] > 0:
            report += f"**Asset Details:**\n"
            if data['weth_supplied'] > 0:
                report += f"  • WETH Supplied: {data['weth_supplied']:.4f} WETH\n"
                report += f"  • WETH Balance: {data['weth_balance']:.4f} WETH\n"
                report += f"  • WETH Price: ${data['weth_price']:.2f}\n"
            if data['gho_borrowed'] > 0:
                report += f"  • GHO Borrowed: {data['gho_borrowed']:.2f} GHO\n"
                report += f"  • GHO Balance: {data['gho_balance']:.2f} GHO\n"
                report += f"  • GHO Price: ${data['gho_price']:.2f}\n"
            report += "\n"
        
        # Position Health
        report += f"**Position Health:**\n"
        report += f"  • Collateralization Ratio: {data['collateralization_ratio']:.2f}x\n"
        report += f"  • Health Threshold: {self.health_threshold:.2f}\n"
        report += f"  • Health Factor: {data['health_factor']:.2f}\n"

        if data['is_healthy']:
            report += f"  • Status: 🟢 **HEALTHY**\n"
        else:
            report += f"  • Status: 🔴 **AT RISK**\n"
        
        return report
    
    def check_position_health(self) -> Dict[str, Any]:
        """Check if position is healthy and return alert data if needed"""
        data = self.get_position_data()
        
        if 'error' in data:
            return {
                'is_healthy': False,
                'alert': True,
                'message': f"Error monitoring position: {data['error']}",
                'severity': 'error'
            }
        
        if not data['is_healthy']:
            return {
                'is_healthy': False,
                'alert': True,
                'message': f"⚠️ **POSITION AT RISK** ⚠️\n"
                          f"Health factor: {data['health_factor']:.2f}\n"
                          f"Collateralization ratio: {data['collateralization_ratio']:.2f}x\n"
                          f"Threshold: {self.health_threshold:.2f}",
                'severity': 'warning',
                'data': data
            }
        
        return {
            'is_healthy': True,
            'alert': False,
            'data': data
        }

# Global instance
cdp_monitor = None

def get_cdp_monitor() -> CDPMonitor:
    """Get or create CDP monitor instance"""
    global cdp_monitor
    if cdp_monitor is None:
        cdp_monitor = CDPMonitor()
    return cdp_monitor

def generate_cdp_report() -> str:
    """Generate CDP position report for Discord bot"""
    try:
        monitor = get_cdp_monitor()
        return monitor.generate_position_report()
    except Exception as e:
        return f"❌ Error initializing CDP monitor: {str(e)}"

def check_cdp_health() -> Dict[str, Any]:
    """Check CDP position health for monitoring/alerts"""
    try:
        monitor = get_cdp_monitor()
        return monitor.check_position_health()
    except Exception as e:
        return {
            'is_healthy': False,
            'alert': True,
            'message': f"❌ Error checking CDP health: {str(e)}",
            'severity': 'error'
        } 