namespace Onion.Repositories.Tests.Docker;

using System;
using System.ComponentModel.DataAnnotations;
using System.Diagnostics;
using FluentAssertions;
using Polly;

[Collection("DockerCompose Tests")]
public class BulkInsertTests
{
    private readonly IRepository sut;
    private static Random _random = new Random();
    public readonly PostgresConnectionFactory ConnectionFactory;

    public BulkInsertTests(OnionDatabaseFixture dbFixture)
    {
        ConnectionFactory = dbFixture.Database.ConnectionFactory;
        sut = new QuoteScenarioRepository(ConnectionFactory);
        Policy
            .Handle<Exception>()
            .WaitAndRetry(3, (_) => TimeSpan.FromMilliseconds(500))
            .Execute(() =>
            {
                dbFixture.Truncate(OnionDatabaseFixture.TestCaseScenarioTable).Wait();
            });
    }

    #region Helper Test Methods
    private TestCase GetTestCasedataSingleRow() =>
        new TestCase(
            "Test_Scenario_Name_01",
            "GBTOGB",
            Guid.NewGuid(),
            "GB",
            "",
            "TW88HR",
            "OriginAddType",
            "GB",
            "",
            "TW88hr",
            "DestinateAddressType",
            1,
            1,
            1,
            1,
            1,
            "parcel",
            "contentType",
            Guid.NewGuid(),
            Guid.NewGuid(),
            true,
            10
        );

    private TestCase[] GenerateRandomDataForQuoteScenariosList(int noOfRowsToBeGenerated)
    {
        TestCase[] quoteScenarioRows = new TestCase[noOfRowsToBeGenerated];

        for (int i = 0; i < noOfRowsToBeGenerated; i++)
        {
            quoteScenarioRows[i] = new TestCase(
                "GB2GB",
                $"testcase{i}",
                Guid.NewGuid(),
                "GB",
                "",
                "TW88HR",
                "OriginAddressType",
                "GB",
                "",
                "TW88HR",
                "DestinationType",
                1,
                1,
                1,
                1,
                1,
                "parcel",
                "contentType",
                Guid.NewGuid(),
                Guid.NewGuid(),
                i % 2 != 0,
                _random.Next(1, 100)
            );
        }

        return quoteScenarioRows;
    }

    #endregion

    [Fact]
    public async Task BulkInsertMethod_InsertsBulkRecords_ShouldInsertFasterThanConventionalForEachInsert()
    {
        //Arrange
        //Generate 1000 Rows
        var testCasesData = GenerateRandomDataForQuoteScenariosList(1000)?.ToList();
        await QuoteScenarioRepository.TruncateTable(
            OnionDatabaseFixture.TestCaseScenarioTable,
            ConnectionFactory
        );
        Stopwatch stopwatch = new Stopwatch();
        stopwatch.Start();

        ArgumentNullException.ThrowIfNull(testCasesData);
        await QuoteScenarioRepository.Insert(
            quoteScenarios: testCasesData?.ToList(),
            ConnectionFactory
        );
        stopwatch.Stop();

        long timeElapsedForLoopData = stopwatch.ElapsedMilliseconds;
        stopwatch.Reset();

        await QuoteScenarioRepository.TruncateTable(
            OnionDatabaseFixture.TestCaseScenarioTable,
            ConnectionFactory
        );

        //Act
        stopwatch.Start();
        await QuoteScenarioRepository.BulkInsert(
            testCasesData?.ToList(),
            OnionDatabaseFixture.TestCaseScenarioTable,
            ConnectionFactory
        );
        stopwatch.Stop();
        long timeElapsetForBulkInsert = stopwatch.ElapsedMilliseconds;

        //Assert
        Assert.True(timeElapsedForLoopData > timeElapsetForBulkInsert);
    }

    [Fact]
    public async Task BulkInsertMethod_GeneratesSQL_InsertSQLWithNullableAndNonNullablePrimitiveDataTypes()
    {
        //Arrange
        string tableName = "public.\"SampleNullableDataTypes\"";
        string generalSchemaSQL =
            $@"

        --Table: public.SampleNullableDataTypes

        DROP TABLE IF EXISTS {tableName};

        CREATE TABLE IF NOT EXISTS {tableName}
        (
            ""PrimayKeyId"" integer NOT NULL GENERATED ALWAYS AS IDENTITY ( INCREMENT 1 START 1 MINVALUE 1 MAXVALUE 2147483647 CACHE 1 ),
            ""MandatoryGuidValue"" uuid NOT NULL,
            ""NUllableGuidValue"" uuid,
            ""MandatoryStringType"" text COLLATE pg_catalog.""default"" NOT NULL,
            ""NullableStringType"" text COLLATE pg_catalog.""default"",
            ""MandatoryIntegerValue"" integer NOT NULL,
            ""NullableIntegerValue"" integer,
            ""MandatoryDecimalValue"" numeric NOT NULL,
            ""NullableDecimalValue"" numeric,
            ""CreatedDate"" timestamp without time zone NOT NULL,
            ""LastModifiedDate"" timestamp without time zone,
            ""MandatoryDoubleType"" double precision NOT NULL,
            ""NullableDoubleType"" double precision,
            CONSTRAINT ""SampleNullableDataTypes_pkey"" PRIMARY KEY (""PrimayKeyId"")
        )

        TABLESPACE pg_default;

        ALTER TABLE IF EXISTS {tableName}
            OWNER to postgres;
";

        await QuoteScenarioRepository.ExecuteRawAsync(generalSchemaSQL, ConnectionFactory);

        SampleNullableDataTypes sampleNullableDataTypes = new SampleNullableDataTypes
        {
            LastModifiedDate = null,
            CreatedDate = DateTime.Now,
            MandatoryDecimalValue = 1,
            MandatoryDoubleType = 1.1,
            NullableDoubleType = null,
            MandatoryGuidValue = Guid.NewGuid(),
            NUllableGuidValue = null,
            MandatoryIntegerValue = 1,
            NullableIntegerValue = null,
            MandatoryStringType = "Test String",
            NullableStringType = null,
            NullableDecimalValue = null
        };

        string bulkInsertSQL = await BulkInsertExtensions.GenerateSQL<SampleNullableDataTypes>(
            new List<SampleNullableDataTypes> { sampleNullableDataTypes },
            tableName
        );

        await QuoteScenarioRepository.ExecuteRawAsync(bulkInsertSQL, ConnectionFactory);

        var result = (
            await QuoteScenarioRepository.GetTsAsync<SampleNullableDataTypes>(
                $"select * from {tableName}",
                ConnectionFactory
            )
        ).Where(x => x.MandatoryGuidValue == sampleNullableDataTypes.MandatoryGuidValue);

        result.Should().HaveCountGreaterThanOrEqualTo(1);
        result
            .Should()
            .Match(x => x.First().MandatoryGuidValue == sampleNullableDataTypes.MandatoryGuidValue);
    }
}

public class SampleNullableDataTypes
{
    [Editable(allowEdit: false)]
    public int PrimayKeyId { get; set; }

    public Guid MandatoryGuidValue { get; set; } = Guid.NewGuid();
    public Guid? NUllableGuidValue { get; set; }
    public string MandatoryStringType { get; set; } = "Mandatory String Value";
    public string? NullableStringType { get; set; }
    public int MandatoryIntegerValue { get; set; } = 10;
    public int? NullableIntegerValue { get; set; }
    public decimal MandatoryDecimalValue { get; set; } = 11m;
    public decimal? NullableDecimalValue { get; set; }
    public DateTime CreatedDate { get; set; } = DateTime.Now;
    public DateTime? LastModifiedDate { get; set; }
    public double MandatoryDoubleType { get; set; } = 12;
    public double? NullableDoubleType { get; set; }
}
