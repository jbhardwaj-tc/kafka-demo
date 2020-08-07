namespace Api
{
    public class OrderRequest
    {
        public int OrderId { get; set; }
        public string ProductName { get; set; }
        public decimal Price { get; set; }
        public Status Status { get; set; }
    }

    public enum Status
    {
        Submitted,
        Completed
    }
}
