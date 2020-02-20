from audit.campaign import DataSource

# Compare the campaign report to report generated via spark
campaign_report = DataSource().campaign_report()

print("All good")
