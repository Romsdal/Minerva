using System;

namespace Minerva.DB_Server;

public class TPCCRandom
{
    private readonly Random _random;
    
    // TPC-C constants for non-uniform distributions
    private static readonly int[] C_VALUES = { 0, 0, 0 }; // Will be initialized with random values
    private static bool _initialized = false;
    private static readonly object _lock = new object();
    
    // Character sets for string generation
    private const string READABLE_CHARS = "0123456789@ABCDEFGHIJKLMNOPQRSTUVWXYZ_abcdefghijklmnopqrstuvwxyz";
    private const string ALPHA_CHARS = "ABCDEFGHIJKLMNOPQRSTUVWXYZ";
    private const string NUMERIC_CHARS = "0123456789";
    
    // Name syllables for TPC-C customer names
    private static readonly string[] NAME_SYLLABLES = 
    {
        "BAR", "OUGHT", "ABLE", "PRI", "PRES", "ESE", "ANTI", "CALLY", "ATION", "EING"
    };

    public TPCCRandom()
    {
        _random = new Random();
        InitializeCValues();
    }

    public TPCCRandom(int seed)
    {
        _random = new Random(seed);
        InitializeCValues();
    }
    
    private void InitializeCValues()
    {
        lock (_lock)
        {
            if (!_initialized)
            {
                C_VALUES[0] = RandInt(0, 255); // For customer last names (load)
                C_VALUES[1] = RandInt(0, 1023); // For customer IDs  
                C_VALUES[2] = RandInt(0, 8191); // For item IDs
                _initialized = true;
            }
        }
    }

    // Basic random number generation
    public int RandInt(int min, int max)
    {
        return _random.Next(min, max + 1);
    }

    public double RandDouble(double min, double max)
    {
        return min + (_random.NextDouble() * (max - min));
    }

    public decimal RandDecimal(decimal min, decimal max, int decimalPlaces)
    {
        var value = (decimal)RandDouble((double)min, (double)max);
        return Math.Round(value, decimalPlaces);
    }

    // Fixed NURand implementation (corrected from original)
    public int NURand(int A, int C, int min, int max)
    {
        int random1 = RandInt(0, A);
        int random2 = RandInt(min, max);
        return (((random1 | random2) + C) % (max - min + 1)) + min;
    }

    // String generation functions
    public string RandomStr(int length)
    {
        if (length <= 0) return "";
        
        var result = new char[length];
        for (int i = 0; i < length; i++)
        {
            result[i] = READABLE_CHARS[_random.Next(READABLE_CHARS.Length)];
        }
        return new string(result);
    }

    public string RandomNStr(int length)
    {
        if (length <= 0) return "";
        
        var result = new char[length];
        for (int i = 0; i < length; i++)
        {
            result[i] = NUMERIC_CHARS[_random.Next(NUMERIC_CHARS.Length)];
        }
        return new string(result);
    }

    public string RandomAlphaStr(int length)
    {
        if (length <= 0) return "";
        
        var result = new char[length];
        for (int i = 0; i < length; i++)
        {
            result[i] = ALPHA_CHARS[_random.Next(ALPHA_CHARS.Length)];
        }
        return new string(result);
    }

    // TPC-C specific random generation functions
    public string LastName(int num)
    {
        string name = "";
        name += NAME_SYLLABLES[num / 100 % 10];
        name += NAME_SYLLABLES[num / 10 % 10];
        name += NAME_SYLLABLES[num % 10];
        return name;
    }

    public string GetNonUniformCustomerLastNameLoad()
    {
        return LastName(NURand(255, C_VALUES[0], 0, 999));
    }

    public string GetNonUniformCustomerLastNameRun()
    {
        return LastName(NURand(255, 223, 0, 999)); // Different C value for runtime
    }

    public int GetCustomerId()
    {
        return NURand(1023, C_VALUES[1], 1, 3000);
    }

    public int GetItemId()
    {
        return NURand(8191, C_VALUES[2], 1, 100000);
    }

    // Probabilistic data generation functions
    public string GenerateCreditStatus()
    {
        // 10% chance for "BC" (bad credit), 90% for "GC" (good credit)
        return RandInt(1, 100) <= 10 ? "BC" : "GC";
    }

    public decimal GenerateDiscountRate()
    {
        // TPC-C: NUMERIC(4,4) range [0.0000..0.5000]
        return RandDecimal(0.0000m, 0.5000m, 4);
    }

    public decimal GenerateTaxRate()
    {
        // TPC-C: NUMERIC(4,4) range [0.0000..0.2000]
        return RandDecimal(0.0000m, 0.2000m, 4);
    }

    public string GenerateStockDataWithOriginal(int length)
    {
        if (length < 8) return RandomStr(length);
        
        // 10% chance to include "ORIGINAL" substring
        if (RandInt(1, 100) <= 10)
        {
            int startPos = RandInt(0, length - 8);
            string prefix = RandomStr(startPos);
            string suffix = RandomStr(length - startPos - 8);
            return prefix + "ORIGINAL" + suffix;
        }
        else
        {
            return RandomStr(length);
        }
    }

    public string GeneratePhoneNumber()
    {
        return RandomNStr(16);
    }

    public string GenerateZipCode()
    {
        return RandomNStr(4) + "11111";
    }

    public string GenerateState()
    {
        return RandomAlphaStr(2);
    }

    public int GenerateStockQuantity()
    {
        return RandInt(10, 100);
    }

    public string GenerateDistInfo()
    {
        return RandomStr(24);
    }

    // Utility functions for data generation ranges
    public string GenerateCustomerFirstName()
    {
        return RandomStr(RandInt(8, 16));
    }

    public string GenerateCustomerStreet()
    {
        return RandomStr(RandInt(10, 20));
    }

    public string GenerateCustomerCity()
    {
        return RandomStr(RandInt(10, 20));
    }

    public string GenerateItemName()
    {
        return RandomStr(RandInt(14, 24));
    }

    public string GenerateItemData()
    {
        int length = RandInt(26, 50);
        return GenerateStockDataWithOriginal(length);
    }

    public string GenerateWarehouseName()
    {
        return RandomStr(RandInt(6, 10));
    }

    public string GenerateDistrictName()
    {
        return RandomStr(RandInt(6, 10));
    }

    public decimal GenerateItemPrice()
    {
        return RandDecimal(1.00m, 100.00m, 2);
    }

    public long GenerateCustomerCreditLimit()
    {
        return RandInt(50000, 500000);
    }

    public decimal GenerateCustomerBalance()
    {
        return RandDecimal(-1000.00m, 1000.00m, 2);
    }

    public decimal GenerateCustomerYtdPayment()
    {
        return RandDecimal(0.00m, 5000.00m, 2);
    }

    public int GenerateCustomerPaymentCount()
    {
        return RandInt(0, 20);
    }

    public int GenerateCustomerDeliveryCount()
    {
        return RandInt(0, 10);
    }

    public string GenerateCustomerData()
    {
        // TPC-C: VARCHAR(500), can be empty or contain up to 500 random characters
        int length = RandInt(0, 500);
        return length == 0 ? "" : RandomStr(length);
    }
}