// This code is property of the GGAO // 


using Microsoft.EntityFrameworkCore.Migrations;

namespace techhubapigw.Migrations
{
    public partial class TenantEntryPoints : Migration
    {
        protected override void Up(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.AddColumn<string>(
                name: "EntryPoints",
                table: "Tenants",
                nullable: true);
        }

        protected override void Down(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.DropColumn(
                name: "EntryPoints",
                table: "Tenants");
        }
    }
}
