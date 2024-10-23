// This code is property of the GGAO // 


using Microsoft.AspNetCore.Identity.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.ChangeTracking;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.Json;
using System.Threading.Tasks;
using techhubapigw.Database.DTOs;

namespace techhubapigw.Database
{
    public class AppDbContext : DbContext //IdentityDbContext<> 
    {
        // Entitites
        public DbSet<Tenant> Tenants { get; set; }
        public DbSet<ApiKeyDTO> ApiKeys { get; set; }
        public DbSet<UsageLimit> Limits { get; set; }
        public DbSet<Metric> Metrics { get; set; }

        public AppDbContext(DbContextOptions<AppDbContext> options) : base(options)
        {
        }

        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            base.OnModelCreating(modelBuilder);

            modelBuilder.Entity<Tenant>(e =>
            {
                e.HasKey(u => u.TenantId);

                // Variable serialization
                e.Property(e => e.EntryPoints)
                    .HasConversion(
                        v => JsonSerializer.Serialize(v, null),
                        v => JsonSerializer.Deserialize<Dictionary<string, string>>(v, null))
                    .Metadata.SetValueComparer(new ValueComparer<Dictionary<string,string>>(
                        (c1, c2) => c1.SequenceEqual(c2),
                        c => c.Aggregate(0, (a, v) => HashCode.Combine(a, v.GetHashCode())),
                        c => c.ToDictionary(kvp => kvp.Key, kvp=> kvp.Value)));

                // Relationships
                e.HasMany(u => u.ApiKeys).WithOne(u => u.Tenant).HasForeignKey(u => u.TenantId);
            });

            modelBuilder.Entity<ApiKeyDTO>(e =>
            {
                e.HasKey(u => u.Key);

                // Additional indexes to improve query time
                e.HasIndex(u => u.ReportId);
                e.HasIndex(u => u.TenantId);
                e.HasIndex(u => u.Department);

                e.Property(e => e.Roles)
                    .HasConversion(
                        v => string.Join(',', v),
                        v => new List<string>(v.Split(',', StringSplitOptions.RemoveEmptyEntries)))
                    .Metadata.SetValueComparer(new ValueComparer<List<string>>(
                        (c1, c2) => c1.SequenceEqual(c2), 
                        c => c.Aggregate(0, (a, v) => HashCode.Combine(a, v.GetHashCode())),
                        c => c.ToList()));

                // Relationships
                e.HasMany(e => e.Limits).WithOne(e => e.ApiKey).HasForeignKey(e => e.ApiKeyId).OnDelete(DeleteBehavior.Cascade);
                e.HasMany(e => e.Metrics).WithOne(e => e.Apikey).HasForeignKey(e => e.ApiKeyId).OnDelete(DeleteBehavior.Cascade);
            });

            // Fast queries
            modelBuilder.Entity<Metric>(e =>
            {
                e.HasIndex(u => u.ApiKeyId);
                e.HasIndex(u => u.Timestamp);
            });



            // TODO: add other entity models configuration
        }
    }
}
