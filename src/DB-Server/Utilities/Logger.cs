using System;
using System.IO;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace Minerva.DB_Server;

public static class LoggerManager
{
    public static IConfiguration LogConfig = null;
    public static ILoggerFactory LoggerFactory = null;
    public static int NodeId { get; set; } = -1; 
    public static string LoggingLevel = "-1";

    public static string ConfigPath { get; set; }

    public static void ConfigureLogger(string ConfigPath = null)
    {
        if (LogConfig is null)
        {
            if (ConfigPath is not null)
            {
                var builder = new ConfigurationBuilder()
                    .SetBasePath(Directory.GetCurrentDirectory())
                    .AddJsonFile(ConfigPath);

                LogConfig = builder.Build().GetSection("Logging");
            }
        }

        if (LoggerFactory is null)
        {
            LoggerFactory = Microsoft.Extensions.Logging.LoggerFactory.Create(builder =>
            {
                builder.AddConfiguration(LogConfig);
                builder.AddSimpleConsole(options =>
                {
                    var timestampFormat = LogConfig["Logging:Console:FormatterOptions:TimestampFormat"];
                    options.TimestampFormat = !string.IsNullOrEmpty(timestampFormat) ? $"[{timestampFormat}]" : "[HH:mm:ss.fff]";
                });
            });
            
        }
    }

    public static ILogger GetLogger(int levelOverride = -1)
    {
        if (levelOverride != -1)
        {
            LoggingLevel = levelOverride.ToString();
        }

        ILogger logger;

        if (LoggingLevel == "-1")
        {

            if (LogConfig is null)
            {
                //Console.WriteLine("No logging level configured, not outputting any log.");
                return new NullLogger<Program>();
            }

            logger = LoggerFactory.CreateLogger($"Node{NodeId}");
        }
        else
        {
            var loggerFactory = Microsoft.Extensions.Logging.LoggerFactory.Create(builder =>
            {
                var logLevel = LoggingLevel switch
                {
                    "0" or "1" or "2" or "3" or "4" or "5" or "6" => Enum.Parse<LogLevel>(LoggingLevel),
                    _ => LogLevel.Debug,
                };
                builder.AddSimpleConsole(options => { options.TimestampFormat = "[HH:mm:ss.fff]"; }).SetMinimumLevel(logLevel);

            });
            logger = loggerFactory.CreateLogger($"Node{NodeId}");
        }

        return logger;
    }
}