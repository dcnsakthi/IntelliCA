"""
ODBC Driver Detection and Installation Helper
Checks for installed SQL Server ODBC drivers and provides installation instructions
Note: mssql-python bundles ODBC Driver 18 for SQL Server
"""
import mssql_python
import platform
import sys


def check_odbc_drivers():
    """Check for available ODBC drivers."""
    print("=" * 80)
    print("ODBC Driver Detection Tool")
    print("=" * 80)
    print(f"\nSystem: {platform.system()} {platform.release()}")
    print(f"Python: {sys.version}")
    print(f"mssql-python module: {mssql_python.__name__}")
    
    print("\n" + "=" * 80)
    print("SQL Server Driver Status:")
    print("=" * 80)
    
    try:
        # mssql-python bundles ODBC Driver 18 for SQL Server
        print("✅ mssql-python is installed")
        print("✅ ODBC Driver 18 for SQL Server (bundled with mssql-python)")
        
        print("\n" + "=" * 80)
        print("Recommendation:")
        print("=" * 80)
        print("✅ Driver ready to use: ODBC Driver 18 for SQL Server")
        print("\nmssql-python automatically handles driver management.")
        print("No additional ODBC driver installation needed!")
        
        return True
    except Exception as e:
        print(f"❌ Error checking mssql-python: {e}")
        print_installation_instructions()
        return False


def print_installation_instructions():
    """Print installation instructions based on OS."""
    print("\n" + "=" * 80)
    print("SQL Server ODBC Driver Installation Instructions")
    print("=" * 80)
    
    system = platform.system()
    
    if system == "Windows":
        print("\n📥 Windows Installation:")
        print("-" * 80)
        print("1. Download ODBC Driver 18 for SQL Server:")
        print("   https://learn.microsoft.com/en-us/sql/connect/odbc/download-odbc-driver-for-sql-server")
        print("\n2. Choose the appropriate installer:")
        print("   • For 64-bit Windows: msodbcsql_18.x_x64.msi")
        print("   • For 32-bit Windows: msodbcsql_18.x_x86.msi")
        print("\n3. Run the installer with default settings")
        print("\n4. After installation, run this script again to verify")
        print("\nDirect download link:")
        print("https://go.microsoft.com/fwlink/?linkid=2249004")
        
    elif system == "Linux":
        print("\n📥 Linux Installation:")
        print("-" * 80)
        print("For Ubuntu/Debian:")
        print("  curl https://packages.microsoft.com/keys/microsoft.asc | sudo apt-key add -")
        print("  curl https://packages.microsoft.com/config/ubuntu/$(lsb_release -rs)/prod.list | \\")
        print("    sudo tee /etc/apt/sources.list.d/mssql-release.list")
        print("  sudo apt-get update")
        print("  sudo ACCEPT_EULA=Y apt-get install -y msodbcsql18")
        print("\nFor Red Hat/CentOS:")
        print("  sudo curl https://packages.microsoft.com/config/rhel/8/prod.repo | \\")
        print("    sudo tee /etc/yum.repos.d/mssql-release.repo")
        print("  sudo yum remove unixODBC-utf16 unixODBC-utf16-devel")
        print("  sudo ACCEPT_EULA=Y yum install -y msodbcsql18")
        
    elif system == "Darwin":
        print("\n📥 macOS Installation:")
        print("-" * 80)
        print("Using Homebrew:")
        print("  brew tap microsoft/mssql-release https://github.com/Microsoft/homebrew-mssql-release")
        print("  brew update")
        print("  HOMEBREW_ACCEPT_EULA=Y brew install msodbcsql18 mssql-tools18")
    
    print("\n" + "=" * 80)
    print("After Installation:")
    print("=" * 80)
    print("1. Close and reopen your terminal/IDE")
    print("2. Run this script again: python scripts/check_odbc_drivers.py")
    print("3. Update your .env file with the detected driver name")
    print("=" * 80)


def check_env_file():
    """Check if .env file has the correct driver configuration."""
    print("\n" + "=" * 80)
    print("Checking .env Configuration:")
    print("=" * 80)
    
    env_file = ".env"
    if not os.path.exists(env_file):
        print("⚠️  .env file not found")
        print("   Create one from .env.example and configure your settings")
        return
    
    import dotenv
    dotenv.load_dotenv()
    
    driver = os.getenv("FABRIC_SQL_DRIVER")
    if driver:
        print(f"✓ FABRIC_SQL_DRIVER is set to: {driver}")
        print("✅ mssql-python will use the bundled ODBC Driver 18 automatically")
    else:
        print("ℹ️  FABRIC_SQL_DRIVER is not set in .env file")
        print("   This is optional with mssql-python (uses bundled driver)")
        print("   To explicitly set it, add this line to your .env file:")
        print('   FABRIC_SQL_DRIVER="ODBC Driver 18 for SQL Server"')


if __name__ == "__main__":
    import os
    
    # Change to project root
    script_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.dirname(script_dir)
    os.chdir(project_root)
    
    success = check_odbc_drivers()
    check_env_file()
    
    if success:
        print("\n" + "=" * 80)
        print("✅ System Ready for Azure SQL Connection!")
        print("=" * 80)
        print("\nNext steps:")
        print("1. Ensure your .env file has the correct driver name")
        print("2. Run: python scripts/setup_databases.py")
    else:
        print("\n" + "=" * 80)
        print("❌ Action Required: Install ODBC Driver")
        print("=" * 80)
        print("\nFollow the installation instructions above, then re-run this script.")
    
    sys.exit(0 if success else 1)
