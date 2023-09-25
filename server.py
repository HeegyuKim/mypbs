import streamlit as st
from mypbs import get_args, MyPBS
import pandas as pd


st.title("MyPBS")

@st.cache_resource()
def get_pbs():
    args = get_args()
    return MyPBS(args.name, args.host, args.port)

pbs = get_pbs()

with st.form("add_job"):
    name = st.text_input("Write a job name")
    command = st.text_area("Write a command")
    submit = st.form_submit_button()

    if submit:
        if name and command:
            pbs.add_job(name, command)
            st.success("Your job is submitted")
        else:
            st.error("Please fill the contents of job name and command.")

refresh = st.button("refresh")
if refresh:
    st.experimental_rerun()

finished = pbs.get_finished_jobs()
nodes = pbs.get_nodes()
waiting = pbs.get_waiting_jobs()

st.markdown("## Node")
st.dataframe(pd.DataFrame({
    "name": nodes.keys(),
    "status": nodes.values()
}))

st.markdown("## Waiting jobs")
st.dataframe(pd.DataFrame(waiting))

st.markdown("## Finished jobs")
st.dataframe(pd.DataFrame(finished))

delete_all = st.button("caution: delete all")
if delete_all:
    delete_all = st.button("caution: delete all!!!! really??!!!!?!?")
    if delete_all:
        pbs.delete()
        st.experimental_rerun()