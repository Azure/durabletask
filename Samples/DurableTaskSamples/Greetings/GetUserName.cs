namespace DurableTaskSamples.Greetings
{
    using System;
    using System.Collections.Generic;
    using System.ComponentModel;
    using System.Data;
    using System.Drawing;
    using System.Linq;
    using System.Text;
    using System.Threading.Tasks;
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
