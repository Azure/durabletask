namespace DurableTaskSamples.Greetings
{
    using System.Windows.Forms;

    public partial class GetUserName : Form
    {
        public GetUserName()
        {
            InitializeComponent();
        }

        public string UserName
        {
            get { return this.txtUserName.Text; }
            set { this.txtUserName.Text = value;  }
        }
    }
}
